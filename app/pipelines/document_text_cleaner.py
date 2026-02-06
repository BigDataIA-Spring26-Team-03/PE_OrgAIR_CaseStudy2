from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService


# -----------------------------
# Helpers
# -----------------------------
def row_get(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def normalize_ws(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def processed_s3_key(doc_id: str) -> str:
    # Stable + idempotent output location (no path reconstruction)
    return f"processed/{doc_id}.txt.gz"


# -----------------------------
# SEC header / boilerplate drops (best-effort)
# -----------------------------
HEADER_PATTERNS = [
    re.compile(r"^UNITED STATES SECURITIES AND EXCHANGE COMMISSION", re.I),
    re.compile(r"^WASHINGTON,\s*D\.C\.\s*20549", re.I),
    re.compile(r"^FORM\s+(10-K|10-Q|8-K|DEF\s*14A)\b", re.I),
    re.compile(r"^Commission File Number", re.I),
    re.compile(r"^Securities registered pursuant to Section", re.I),
    re.compile(r"^Indicate by check mark", re.I),
    re.compile(r"^☐|^☑", re.I),
    re.compile(r"^TABLE OF CONTENTS\b", re.I),
    re.compile(r"^INDEX TO FINANCIAL STATEMENTS\b", re.I),
]

GARBAGE_LINE_PATTERNS = [
    re.compile(r"^[-_]{8,}$"),
    re.compile(r"^\s*\d+\s*$"),
    re.compile(r"^\s*Page\s+\d+\s*$", re.I),
    re.compile(r"^https?://\S+$", re.I),
    re.compile(r"^\s*(xbrl|ixbrl|inline xbrl)\s*$", re.I),
    re.compile(r"^\s*<[^>]+>\s*$"),  # stray tags
]

INVENTORY_LINE_PATTERNS = [
    # "EX-4.1 exhibit41q4fy25.htm"
    re.compile(
        r"^EX-\d+(?:\.\w+)?\s+\S+\.(?:htm|html|xml|xsd|xbrl|jpg|jpeg|png|gif|pdf|txt)\s*$",
        re.I,
    ),
    # filename-only lines
    re.compile(
        r"^[A-Za-z0-9][A-Za-z0-9._-]{2,}\.(?:htm|html|xml|xsd|xbrl|jpg|jpeg|png|gif|pdf|txt)\s*$",
        re.I,
    ),
    re.compile(r"^XBRL\s+TAXONOMY\s+EXTENSION\s+.*$", re.I),
    re.compile(r"^GRAPHIC(?:\s+\S+)?\s*$", re.I),
]

# -----------------------------
# Binary/uuencode attachment killing (CRITICAL)
# -----------------------------
UUE_BEGIN_RE = re.compile(r"^begin\s+\d{3}\s+.+$", re.I)
UUE_END_RE = re.compile(r"^end\s*$", re.I)
UUE_MLINE_RE = re.compile(r"^M[\x20-\x7E]{55,}$")
HIGH_SYMBOL_RE = re.compile(r"^[A-Za-z0-9+/=]{0,10}[^A-Za-z0-9\s]{10,}.*$")
REPEAT_GIBBERISH_RE = re.compile(r'^(?:[A-Z@]{3,}|\*{3,}|"+|[A-Z]{2,}@)\s*.*$', re.I)


def is_binary_like_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return False

    if UUE_BEGIN_RE.match(s) or UUE_END_RE.match(s):
        return True
    if UUE_MLINE_RE.match(s):
        return True

    if len(s) >= 80:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha / max(len(s), 1) < 0.12:
            return True

    nonword = sum(1 for ch in s if not (ch.isalnum() or ch.isspace()))
    if len(s) >= 60 and (nonword / len(s)) > 0.35:
        return True

    if HIGH_SYMBOL_RE.match(s) and len(s) >= 40:
        alpha = sum(ch.isalpha() for ch in s)
        if alpha < 15:
            return True

    if len(s) >= 30 and REPEAT_GIBBERISH_RE.match(s):
        alpha = sum(ch.isalpha() for ch in s)
        spaces = s.count(" ")
        if alpha / max(len(s), 1) < 0.25 and spaces < 10:
            return True

    return False


def drop_binary_blocks(text: str) -> str:
    lines = text.splitlines()
    out: list[str] = []
    in_uue_block = False

    for ln in lines:
        s = ln.rstrip("\n")

        if UUE_BEGIN_RE.match(s.strip()):
            in_uue_block = True
            continue

        if in_uue_block:
            if UUE_END_RE.match(s.strip()):
                in_uue_block = False
            continue

        if is_binary_like_line(s):
            continue

        out.append(s)

    cleaned = "\n".join(out)
    return normalize_ws(cleaned)


def clean_sec_text(text: str) -> str:
    text = normalize_ws(text)
    text = drop_binary_blocks(text)

    lines = [ln.strip() for ln in text.splitlines()]
    cleaned_lines: list[str] = []

    for ln in lines:
        if not ln:
            cleaned_lines.append("")
            continue

        if any(p.search(ln) for p in HEADER_PATTERNS):
            continue
        if any(p.search(ln) for p in GARBAGE_LINE_PATTERNS):
            continue
        if any(p.search(ln) for p in INVENTORY_LINE_PATTERNS):
            continue

        if len(ln) >= 10 and len(set(ln)) <= 2:
            continue

        cleaned_lines.append(ln)

    out = "\n".join(cleaned_lines)
    out = normalize_ws(out)

    # kill lingering TOC blobs
    out = re.sub(r"\bTABLE OF CONTENTS\b.*?(?=\n\n)", "", out, flags=re.I | re.S)
    out = normalize_ws(out)
    return out


# -----------------------------
# Pipeline
# -----------------------------
@dataclass(frozen=True)
class CleanResult:
    doc_id: str
    processed_key: str
    cleaned_hash: str
    chars: int


class DocumentTextCleanerPipeline:
    def __init__(self, sf: Optional[SnowflakeService] = None, s3: Optional[S3Storage] = None) -> None:
        self.sf = sf or SnowflakeService()
        self.s3 = s3 or S3Storage()

    def fetch_parsed_documents(self, limit: int = 50) -> list[dict[str, Any]]:
        # Snowflake is source of truth; ONLY status=parsed are candidates
        return self.sf.execute_query(
            """
            SELECT id, ticker, filing_type, s3_key
            FROM documents
            WHERE LOWER(status) = 'parsed'
            ORDER BY created_at
            LIMIT %(limit)s
            """,
            {"limit": int(limit)},
        )

    def set_status(self, doc_id: str, status: str, error_message: Optional[str] = None) -> None:
        self.sf.execute_update(
            """
            UPDATE documents
            SET status=%(status)s,
                error_message=%(error)s,
                processed_at=CURRENT_TIMESTAMP()
            WHERE id=%(id)s
            """,
            {"id": doc_id, "status": status, "error": error_message},
        )

    def update_clean_row(
        self,
        doc_id: str,
        processed_key: str,
        cleaned_hash: str,
        error_message: Optional[str],
    ) -> None:
        # content_hash becomes SHA256(cleaned_text) per spec
        # documents.s3_key becomes processed artifact key per spec
        self.sf.execute_update(
            """
            UPDATE documents
            SET s3_key=%(s3_key)s,
                content_hash=%(hash)s,
                status='cleaned',
                error_message=%(error)s,
                processed_at=CURRENT_TIMESTAMP()
            WHERE id=%(id)s
            """,
            {"id": doc_id, "s3_key": processed_key, "hash": cleaned_hash, "error": error_message},
        )

    def find_duplicate_doc(self, ticker: str, filing_type: str, cleaned_hash: str, current_id: str) -> Optional[dict[str, Any]]:
        rows = self.sf.execute_query(
            """
            SELECT id, s3_key
            FROM documents
            WHERE UPPER(ticker) = UPPER(%(ticker)s)
              AND filing_type = %(filing_type)s
              AND content_hash = %(hash)s
              AND id <> %(id)s
              AND LOWER(status) IN ('cleaned','chunked')
            LIMIT 1
            """,
            {"ticker": ticker, "filing_type": filing_type, "hash": cleaned_hash, "id": current_id},
        )
        return rows[0] if rows else None

    def run(self, limit: int = 50) -> dict[str, int]:
        rows = self.fetch_parsed_documents(limit=limit)
        if not rows:
            print("No documents with status='parsed' to clean.")
            return {"scanned": 0, "cleaned": 0, "deduped": 0, "failed": 0}

        cleaned = 0
        deduped = 0
        failed = 0

        for r in rows:
            doc_id = str(row_get(r, "id", "ID"))
            parsed_key = str(row_get(r, "s3_key", "S3_KEY") or "").strip()
            ticker = str(row_get(r, "ticker", "TICKER") or "").upper()
            filing_type = str(row_get(r, "filing_type", "FILING_TYPE") or "")

            if not parsed_key:
                failed += 1
                self.set_status(doc_id, "clean_error", "clean_failed: missing parsed s3_key")
                print(f"❌ Clean failed: missing parsed s3_key id={doc_id}")
                continue

            out_key = processed_s3_key(doc_id)

            print(f"🧼 Cleaning: {ticker} {filing_type} id={doc_id}")
            t0 = time.time()

            try:
                # Read parsed JSON (auto handles .gz)
                parsed = self.s3.read_json_auto(parsed_key)
                raw_text = (parsed.get("text") or "").strip()
                if not raw_text:
                    raise ValueError("parsed.text is empty")

                cleaned_text = clean_sec_text(raw_text)
                if not cleaned_text.strip():
                    raise ValueError("cleaned_text ended empty")

                cleaned_hash = sha256_text(cleaned_text)

                # Dedup check (Snowflake authority)
                dup = self.find_duplicate_doc(ticker, filing_type, cleaned_hash, doc_id)
                if dup:
                    dup_id = str(row_get(dup, "id", "ID"))
                    dup_s3_key = str(row_get(dup, "s3_key", "S3_KEY") or "").strip()

                    # Prefer reusing existing processed artifact if it exists
                    if dup_s3_key and self.s3.exists(dup_s3_key):
                        self.update_clean_row(
                            doc_id=doc_id,
                            processed_key=dup_s3_key,
                            cleaned_hash=cleaned_hash,
                            error_message=f"dedup: same cleaned_hash as document {dup_id}",
                        )
                        deduped += 1
                        elapsed = time.time() - t0
                        print(f"✅ Cleaned (DEDUP reused): id={doc_id} -> {dup_id} in {elapsed:.1f}s")
                        continue

                    # If dup row exists but artifact missing, we still write ours (correctness > cleverness)
                    # fall through to write out_key

                # Idempotent artifact write
                if not self.s3.exists(out_key):
                    self.s3.put_text(out_key, cleaned_text, gzip_compress=True)

                # Update Snowflake row to cleaned + point at processed artifact
                dup_msg = None
                if dup:
                    dup_id = str(row_get(dup, "id", "ID"))
                    dup_msg = f"dedup: same cleaned_hash as document {dup_id}"

                self.update_clean_row(doc_id, out_key, cleaned_hash, dup_msg)

                elapsed = time.time() - t0
                if dup_msg:
                    deduped += 1
                    print(f"✅ Cleaned (DEDUP marked): {ticker} {filing_type} id={doc_id} in {elapsed:.1f}s")
                else:
                    cleaned += 1
                    print(f"✅ Cleaned: {ticker} {filing_type} id={doc_id} in {elapsed:.1f}s")

            except Exception as e:
                failed += 1
                self.set_status(doc_id, "clean_error", f"clean_failed: {type(e).__name__}: {e}")
                print(f"❌ Clean failed: {ticker} {filing_type} id={doc_id} error={e}")

        print("\n=== CLEANER SUMMARY ===")
        print(f"Scanned: {len(rows)}")
        print(f"Cleaned: {cleaned}")
        print(f"Deduped: {deduped}")
        print(f"Failed: {failed}")
        return {"scanned": len(rows), "cleaned": cleaned, "deduped": deduped, "failed": failed}


def main(limit: int = 50) -> None:
    DocumentTextCleanerPipeline().run(limit=limit)
