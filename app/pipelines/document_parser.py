from __future__ import annotations

import re
import tempfile
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple

import fitz  # PyMuPDF
import pdfplumber
from bs4 import BeautifulSoup

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService


# -----------------------------
# Guardrails (prevent hangs)
# -----------------------------
MAX_HTML_BYTES = 12_000_000          # keep DOM parse bounded (SEC files can be huge)
MAX_TABLES_SCAN = 600
MAX_TABLES_EMIT = 250
MAX_ROWS_PER_TABLE = 200
MAX_CELLS_PER_ROW = 40
PRINT_EVERY_N_TABLES = 50


def parsed_s3_key(raw_key: str) -> str:
    parts = raw_key.split("/")
    if parts:
        parts[0] = "parsed"
    filename = parts[-1]
    base = filename.rsplit(".", 1)[0] if "." in filename else filename
    parts[-1] = f"{base}.json.gz"
    return "/".join(parts)


def normalize(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def row_get(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def looks_like_pdf(data: bytes) -> bool:
    return data[:4] == b"%PDF"


def looks_like_html(data: bytes) -> bool:
    head = data[:8000].lower()
    return (
        b"<html" in head
        or b"<!doctype html" in head
        or b"</table" in head
        or b"<xbrl" in head
        or b"<ix:" in head
        or b"<div" in head
        or b"<p" in head
    )


class DocumentParser:
    """
    SEC-safe parser:
      - HTML/TXT: BeautifulSoup with lxml, fallback to html.parser (prevents hangs)
      - Remove XBRL/inline XBRL
      - Bound table extraction so no doc stalls the run
      - PDF: pdfplumber tables + PyMuPDF fallback text
    """

    XBRL_LIKE_ATTR_RE = re.compile(r"xbrl|ix:", re.I)

    def _strip_xbrl(self, soup: BeautifulSoup) -> int:
        removed = 0

        for tag in soup.find_all(
            ["xbrl", "ix:header", "ix:nonnumeric", "ix:nonfraction", "ix:continuation", "ix:footnote"]
        ):
            tag.decompose()
            removed += 1

        for tag in soup.find_all(attrs={"contextref": True}):
            tag.decompose()
            removed += 1

        for tag in soup.find_all(attrs={"name": self.XBRL_LIKE_ATTR_RE}):
            tag.decompose()
            removed += 1

        for tag in soup.find_all(attrs={"class": re.compile(r"xbrl|ixbrl|inline-xbrl|inlinexbrl", re.I)}):
            tag.decompose()
            removed += 1

        for tag in soup.find_all(attrs={"id": re.compile(r"xbrl|ixbrl|inline-xbrl|inlinexbrl", re.I)}):
            tag.decompose()
            removed += 1

        return removed

    def _make_soup_resilient(self, html: str) -> Tuple[BeautifulSoup, str]:
        """
        Try lxml (fast). If it errors/hangs in practice, fallback to html.parser (tolerant).
        """
        try:
            soup = BeautifulSoup(html, "lxml")
            return soup, "lxml"
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
            return soup, "html.parser"

    def parse_html(self, data: bytes) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        # Bound size to avoid lxml feed pathologies on gigantic malformed docs
        if len(data) > MAX_HTML_BYTES:
            data = data[:MAX_HTML_BYTES]

        # decode once; remove nulls early
        html = data.decode("utf-8", errors="ignore").replace("\x00", " ")

        # build soup with fallback
        soup, builder = self._make_soup_resilient(html)

        # remove obvious noise
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # remove XBRL / inline XBRL
        xbrl_removed = self._strip_xbrl(soup)

        # extract text
        text = normalize(soup.get_text("\n"))

        # extract tables with guardrails
        tables: List[Dict[str, Any]] = []
        all_tables = soup.find_all("table")
        total_tables_found = len(all_tables)
        tables_to_scan = all_tables[:MAX_TABLES_SCAN]

        for t_index, table in enumerate(tables_to_scan):
            if (t_index + 1) % PRINT_EVERY_N_TABLES == 0:
                print(f"   ...HTML tables scanned {t_index + 1}/{min(total_tables_found, MAX_TABLES_SCAN)}")

            if len(tables) >= MAX_TABLES_EMIT:
                break

            rows: List[List[str]] = []
            for tr in table.find_all("tr")[:MAX_ROWS_PER_TABLE]:
                tds = tr.find_all("td")[:MAX_CELLS_PER_ROW]
                if not tds:
                    continue
                cells = [normalize(td.get_text(" ", strip=True)) for td in tds]
                if any(cells):
                    rows.append(cells)

            if rows:
                tables.append({"table_index": t_index, "rows": rows})

        meta = {
            "builder": builder,
            "html_bytes_used": len(data),
            "xbrl_nodes_removed": xbrl_removed,
            "total_tables_found": total_tables_found,
            "tables_scanned": min(total_tables_found, MAX_TABLES_SCAN),
            "tables_emitted": min(len(tables), MAX_TABLES_EMIT),
            "note": "Resilient soup builder + XBRL removed + bounded table extraction.",
        }
        return text, tables, meta

    def parse_pdf(self, data: bytes) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any]]:
        text_parts: List[str] = []
        tables: List[Dict[str, Any]] = []

        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as f:
            f.write(data)
            f.flush()

            with pdfplumber.open(f.name) as pdf:
                for i, page in enumerate(pdf.pages):
                    page_text = page.extract_text(x_tolerance=2, y_tolerance=2)
                    if page_text:
                        text_parts.append(page_text)

                    for table in (page.extract_tables() or [])[:50]:
                        if table:
                            tables.append({"page": i + 1, "rows": table})

            used_fallback = False
            try:
                doc = fitz.open(f.name)
                pymu_text = [p.get_text("text") for p in doc]
                doc.close()

                if not text_parts or len("".join(pymu_text)) > len("".join(text_parts)):
                    text_parts = pymu_text
                    used_fallback = True
            except Exception:
                pass

        meta = {
            "pdf_tables_count": len(tables),
            "pymupdf_fallback_used": used_fallback,
        }
        return normalize("\n".join(text_parts)), tables, meta


class DocumentParserS3Pipeline:
    def __init__(self) -> None:
        self.sf = SnowflakeService()
        self.s3 = S3Storage()
        self.parser = DocumentParser()

    def run(self, limit: int = 50) -> None:
        rows = self.sf.execute_query(
            """
            SELECT id, ticker, filing_type, s3_key
            FROM documents
            WHERE status = 'downloaded'
            ORDER BY created_at
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )

        if not rows:
            print("No documents with status='downloaded' to parse.")
            return

        parsed_count = 0
        skipped_existing = 0
        failed = 0

        for r in rows:
            doc_id = row_get(r, "id", "ID")
            raw_key = row_get(r, "s3_key", "S3_KEY")
            ticker = (row_get(r, "ticker", "TICKER") or "").upper()
            filing_type = row_get(r, "filing_type", "FILING_TYPE") or ""

            if not doc_id or not raw_key:
                raise RuntimeError(f"Malformed documents row (missing id/s3_key): {r}")

            out_key = parsed_s3_key(str(raw_key))

            # Idempotent
            if self.s3.exists(out_key):
                self.sf.execute_update(
                    "UPDATE documents SET status='parsed', error_message=NULL WHERE id=%(id)s",
                    {"id": doc_id},
                )
                skipped_existing += 1
                print(f"↪️  SKIP (already parsed): {ticker} {filing_type} id={doc_id}")
                continue

            print(f"🔎 Parsing: {ticker} {filing_type} id={doc_id} raw={raw_key}")
            t0 = time.time()

            try:
                # If S3 is slow, you'd at least see delay here; if it errors, it fails fast
                data = self.s3.get_bytes(str(raw_key))

                # Decide which parser to use
                if str(raw_key).lower().endswith(".pdf") or looks_like_pdf(data):
                    print("   ...PDF detected")
                    text, tables, meta = self.parser.parse_pdf(data)
                    parser_type = "pdf"
                else:
                    # Some .txt are actually HTML-ish SEC blobs
                    print("   ...HTML/TXT detected (resilient soup)")
                    text, tables, meta = self.parser.parse_html(data)
                    parser_type = "html"

                if not text:
                    raise ValueError("No text extracted")

                payload = {
                    "document_id": doc_id,
                    "ticker": ticker,
                    "filing_type": filing_type,
                    "raw_s3_key": raw_key,
                    "parsed_s3_key": out_key,
                    "parsed_at": datetime.utcnow().isoformat() + "Z",
                    "parser_type": parser_type,
                    "meta": meta,
                    "text": text,
                    "tables": tables,
                }

                self.s3.put_json_gz(out_key, payload)

                self.sf.execute_update(
                    "UPDATE documents SET status='parsed', error_message=NULL WHERE id=%(id)s",
                    {"id": doc_id},
                )

                elapsed = time.time() - t0
                parsed_count += 1
                print(f"✅ Parsed: {ticker} {filing_type} id={doc_id} (tables={len(tables)}) in {elapsed:.1f}s")

            except Exception as e:
                failed += 1
                self.sf.execute_update(
                    "UPDATE documents SET status='error', error_message=%(err)s WHERE id=%(id)s",
                    {"id": doc_id, "err": f"parse_failed: {type(e).__name__}: {e}"},
                )
                print(f"❌ Parse failed: {ticker} {filing_type} id={doc_id} error={e}")
                # continue to next doc (don't crash whole run)

        print("\n=== PARSER SUMMARY ===")
        print(f"Scanned: {len(rows)}")
        print(f"Parsed:  {parsed_count}")
        print(f"Skipped: {skipped_existing}")
        print(f"Failed:  {failed}")


def main(limit: int = 50) -> None:
    DocumentParserS3Pipeline().run(limit)