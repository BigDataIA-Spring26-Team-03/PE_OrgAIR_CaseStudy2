from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from app.services.s3_storage import S3Storage
from app.services.snowflake import SnowflakeService

# -----------------------------
# Grading standards
# -----------------------------
MIN_WORDS = 500
MAX_WORDS = 1000
OVERLAP_WORDS = 75  # 50–100 allowed


# -----------------------------
# Helpers
# -----------------------------
def row_get(row: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def normalize_ws(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def word_count(text: str) -> int:
    return len(text.split())


def filing_type_norm(filing_type: str) -> str:
    return filing_type.upper().strip().replace(" ", "").replace("-", "")


def processed_s3_key_from_raw(raw_key: str) -> str:
    # sec/.../<doc>.<ext> -> processed/.../<doc>.txt.gz
    k = raw_key.replace("sec/", "processed/", 1)
    k = re.sub(r"\.[^./]+$", ".txt.gz", k)
    return k


def take_overlap_words(text: str, overlap: int) -> str:
    words = text.split()
    if not words:
        return ""
    return " ".join(words[-overlap:])


_SENTENCE_SPLIT_RE = re.compile(r"(?<=[\.!?])\s+(?=[A-Z0-9])")


def split_sentences(text: str) -> List[str]:
    text = text.strip()
    if not text:
        return []
    parts = _SENTENCE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def sentence_aware_split(text: str, max_words: int, overlap_words: int) -> List[str]:
    """
    Last resort: split long text by sentence boundaries (and by words if needed).
    """
    sents = split_sentences(text)
    if not sents:
        words = text.split()
        out: List[str] = []
        step = max_words - overlap_words
        i = 0
        while i < len(words):
            out.append(" ".join(words[i : i + max_words]).strip())
            i += step
        return out

    out: List[str] = []
    buf: List[str] = []
    buf_w = 0

    def flush() -> None:
        nonlocal buf, buf_w
        if buf:
            out.append(" ".join(buf).strip())
        buf = []
        buf_w = 0

    for s in sents:
        w = word_count(s)
        if w > max_words:
            flush()
            words = s.split()
            step = max_words - overlap_words
            i = 0
            while i < len(words):
                out.append(" ".join(words[i : i + max_words]).strip())
                i += step
            continue

        if buf_w + w <= max_words:
            buf.append(s)
            buf_w += w
        else:
            flush()
            buf.append(s)
            buf_w = w

    flush()

    # overlap without exceeding max
    if overlap_words > 0 and len(out) > 1:
        overlapped: List[str] = []
        prev = ""
        for c in out:
            if prev:
                ov = take_overlap_words(prev, overlap_words)
                if ov:
                    merged = (ov + " " + c).strip()
                    overlapped.append(merged if word_count(merged) <= max_words else c)
                else:
                    overlapped.append(c)
            else:
                overlapped.append(c)
            prev = c
        out = overlapped

    return out


# -----------------------------
# Section detection
# -----------------------------
@dataclass(frozen=True)
class SectionSlice:
    section: str
    start: int
    end: int
    text: str


def _dedupe_hits(hits: List[Tuple[int, str]]) -> List[Tuple[int, str]]:
    hits.sort(key=lambda x: x[0])
    out: List[Tuple[int, str]] = []
    last = -10_000
    for pos, label in hits:
        if pos - last < 250:
            continue
        out.append((pos, label))
        last = pos
    return out


def find_section_boundaries(text: str, filing_type: str) -> List[Tuple[int, str]]:
    t = "\n" + text + "\n"
    ft = filing_type_norm(filing_type)
    patterns: List[Tuple[str, str]] = []

    if ft == "10K":
        patterns = [
            (r"\n\s*ITEM\s+1\s*[\.\:\-]\s+", "Item 1"),
            (r"\n\s*ITEM\s+1A\s*[\.\:\-]\s+", "Item 1A"),
            (r"\n\s*ITEM\s+1B\s*[\.\:\-]\s+", "Item 1B"),
            (r"\n\s*ITEM\s+7\s*[\.\:\-]\s+", "Item 7"),
            (r"\n\s*ITEM\s+7A\s*[\.\:\-]\s+", "Item 7A"),
        ]
    elif ft == "10Q":
        patterns = [
            (r"\n\s*ITEM\s+1A\s*[\.\:\-]\s+", "Item 1A"),
            (r"\n\s*ITEM\s+2\s*[\.\:\-]\s+", "Item 2"),
        ]
    elif ft == "8K":
        patterns = [
            (r"\n\s*ITEM\s+1\.01\s*[\.\:\-]\s+", "Item 1.01"),
            (r"\n\s*ITEM\s+2\.02\s*[\.\:\-]\s+", "Item 2.02"),
            (r"\n\s*ITEM\s+5\.02\s*[\.\:\-]\s+", "Item 5.02"),
            (r"\n\s*ITEM\s+7\.01\s*[\.\:\-]\s+", "Item 7.01"),
            (r"\n\s*ITEM\s+8\.01\s*[\.\:\-]\s+", "Item 8.01"),
        ]
    elif ft == "DEF14A":
        patterns = [
            (r"\n\s*EXECUTIVE\s+COMPENSATION\s*\n", "Executive Compensation"),
            (r"\n\s*COMPENSATION\s+DISCUSSION\s+AND\s+ANALYSIS\s*\n", "CD&A"),
            (r"\n\s*DIRECTOR\s+COMPENSATION\s*\n", "Director Compensation"),
            (r"\n\s*NAMED\s+EXECUTIVE\s+OFFICERS?\s*\n", "Named Executive Officers"),
            (r"\n\s*PAY\s+VERSUS\s+PERFORMANCE\s*\n", "Pay Versus Performance"),
        ]

    hits: List[Tuple[int, str]] = []
    for pat, label in patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            hits.append((m.start(), label))

    return _dedupe_hits(hits)


def slice_sections(text: str, filing_type: str) -> List[SectionSlice]:
    boundaries = find_section_boundaries(text, filing_type)
    if not boundaries:
        return [SectionSlice(section="Unknown", start=0, end=len(text), text=text)]

    slices: List[SectionSlice] = []
    first_pos = boundaries[0][0]

    if first_pos > 900:
        intro = text[:first_pos].strip()
        if intro:
            slices.append(SectionSlice(section="Intro", start=0, end=first_pos, text=intro))

    for i, (pos, label) in enumerate(boundaries):
        start = max(pos - 1, 0)
        end = boundaries[i + 1][0] if i + 1 < len(boundaries) else len(text)
        sec_text = text[start:end].strip()
        if sec_text:
            slices.append(SectionSlice(section=label, start=start, end=end, text=sec_text))

    return slices if slices else [SectionSlice(section="Unknown", start=0, end=len(text), text=text)]


# -----------------------------
# Semantic blocks
# -----------------------------
def is_noise_block(b: str) -> bool:
    b = b.strip()
    if not b:
        return True

    wc = word_count(b)
    if wc < 10:
        return True

    # table-ish: many short lines
    lines = b.splitlines()
    if len(lines) >= 10:
        short_lines = sum(1 for ln in lines if len(ln.split()) <= 6)
        if short_lines / max(len(lines), 1) > 0.65:
            return True

    # low alpha ratio (rare after cleaner)
    letters = sum(ch.isalpha() for ch in b)
    if wc < 50 and letters / max(len(b), 1) < 0.10:
        return True

    return False


def split_semantic_blocks(text: str) -> List[str]:
    parts = [p.strip() for p in re.split(r"\n\s*\n+", text) if p.strip()]

    blocks: List[str] = []
    carry: List[str] = []

    for p in parts:
        if is_noise_block(p):
            carry.append(p)
            continue

        if carry:
            blocks.append("\n".join(carry + [p]).strip())
            carry = []
        else:
            blocks.append(p)

    if carry:
        if blocks:
            blocks[-1] = (blocks[-1] + "\n" + "\n".join(carry)).strip()
        else:
            blocks = ["\n".join(carry).strip()]

    # merge micro blocks to ~150 words
    merged: List[str] = []
    buf: List[str] = []
    buf_w = 0

    for b in blocks:
        w = word_count(b)
        if buf_w + w < 150:
            buf.append(b)
            buf_w += w
            continue

        if buf:
            merged.append("\n\n".join(buf).strip())
            buf = []
            buf_w = 0

        merged.append(b)

    if buf:
        merged.append("\n\n".join(buf).strip())

    return [m for m in merged if m.strip()]


# -----------------------------
# Chunk builder
# -----------------------------
def build_chunks_for_section(sec_text: str) -> List[str]:
    blocks = split_semantic_blocks(sec_text)
    if not blocks:
        return []

    # expand huge blocks first
    expanded: List[str] = []
    for b in blocks:
        if word_count(b) > MAX_WORDS:
            expanded.extend(sentence_aware_split(b, MAX_WORDS, 0))
        else:
            expanded.append(b)

    chunks: List[str] = []
    buf: List[str] = []
    buf_w = 0

    def flush() -> None:
        nonlocal buf, buf_w
        if not buf:
            return
        c = "\n\n".join(buf).strip()
        if c:
            chunks.append(c)
        buf = []
        buf_w = 0

    for b in expanded:
        w = word_count(b)

        if not buf and MIN_WORDS <= w <= MAX_WORDS:
            chunks.append(b.strip())
            continue

        if buf_w + w <= MAX_WORDS:
            buf.append(b)
            buf_w += w
        else:
            flush()
            buf.append(b)
            buf_w = w
            if buf_w > MAX_WORDS:
                flush()

    flush()

    # merge chunks < MIN_WORDS into neighbors where possible
    merged: List[str] = []
    i = 0
    while i < len(chunks):
        c = chunks[i]
        wc = word_count(c)

        if wc >= MIN_WORDS or len(chunks) == 1:
            merged.append(c)
            i += 1
            continue

        if i + 1 < len(chunks):
            combo = (c + "\n\n" + chunks[i + 1]).strip()
            if word_count(combo) <= MAX_WORDS:
                merged.append(combo)
                i += 2
                continue

        if merged:
            combo = (merged[-1] + "\n\n" + c).strip()
            if word_count(combo) <= MAX_WORDS:
                merged[-1] = combo
                i += 1
                continue

        merged.append(c)
        i += 1

    chunks = merged

    # overlap (no exceed MAX)
    if OVERLAP_WORDS > 0 and len(chunks) > 1:
        out: List[str] = []
        prev = ""
        for c in chunks:
            if prev:
                ov = take_overlap_words(prev, OVERLAP_WORDS)
                if ov:
                    mc = (ov + " " + c).strip()
                    out.append(mc if word_count(mc) <= MAX_WORDS else c)
                else:
                    out.append(c)
            else:
                out.append(c)
            prev = c
        chunks = out

    # last guard
    final: List[str] = []
    for c in chunks:
        if word_count(c) <= MAX_WORDS:
            final.append(c)
        else:
            final.extend(sentence_aware_split(c, MAX_WORDS, OVERLAP_WORDS))

    return [normalize_ws(x) for x in final if normalize_ws(x)]


def find_char_span(doc_text: str, chunk_text: str) -> Tuple[Optional[int], Optional[int]]:
    # don't lie: only set if we find the exact substring
    idx = doc_text.find(chunk_text)
    if idx == -1:
        return None, None
    return idx, idx + len(chunk_text)


# -----------------------------
# Snowflake row
# -----------------------------
@dataclass(frozen=True)
class ChunkRow:
    id: str
    document_id: str
    chunk_index: int
    content: str
    section: str
    start_char: Optional[int]
    end_char: Optional[int]
    word_count: int


# -----------------------------
# Pipeline
# -----------------------------
class DocumentChunkerS3Pipeline:
    def __init__(
        self,
        sf: Optional[SnowflakeService] = None,
        s3: Optional[S3Storage] = None,
    ) -> None:
        self.sf = sf or SnowflakeService()
        self.s3 = s3 or S3Storage.from_env()

    def fetch_cleaned_documents(self, limit: int) -> List[Dict[str, Any]]:
        # escape % for Snowflake pyformat
        return self.sf.execute_query(
            """
            SELECT id, ticker, filing_type, s3_key
            FROM documents
            WHERE status='cleaned'
              AND (error_message IS NULL OR error_message NOT ILIKE 'dedup:%%')
            ORDER BY created_at
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )

    def existing_chunk_count(self, doc_id: str) -> int:
        rows = self.sf.execute_query(
            "SELECT COUNT(*) AS cnt FROM document_chunks WHERE document_id=%(id)s",
            {"id": doc_id},
        )
        if not rows:
            return 0
        v = rows[0].get("CNT") if "CNT" in rows[0] else rows[0].get("cnt")
        return int(v or 0)

    def insert_chunks_batch(self, rows: List[ChunkRow]) -> None:
        if not rows:
            return

        values_sql: List[str] = []
        params: Dict[str, Any] = {}

        for i, ch in enumerate(rows):
            values_sql.append(
                f"(%(id{i})s, %(document_id{i})s, %(chunk_index{i})s, %(content{i})s, %(section{i})s, %(start_char{i})s, %(end_char{i})s, %(word_count{i})s)"
            )
            params[f"id{i}"] = ch.id
            params[f"document_id{i}"] = ch.document_id
            params[f"chunk_index{i}"] = ch.chunk_index
            params[f"content{i}"] = ch.content
            params[f"section{i}"] = ch.section or "Unknown"
            params[f"start_char{i}"] = ch.start_char
            params[f"end_char{i}"] = ch.end_char
            params[f"word_count{i}"] = ch.word_count

        sql = f"""
        INSERT INTO document_chunks
            (id, document_id, chunk_index, content, section, start_char, end_char, word_count)
        VALUES
            {", ".join(values_sql)}
        """
        self.sf.execute_update(sql, params)

    def mark_chunked(self, doc_id: str, chunk_count: int) -> None:
        self.sf.execute_update(
            """
            UPDATE documents
            SET status='chunked',
                chunk_count=%(cnt)s,
                processed_at=CURRENT_TIMESTAMP(),
                error_message=NULL
            WHERE id=%(id)s
            """,
            {"id": doc_id, "cnt": chunk_count},
        )

    def mark_error(self, doc_id: str, msg: str) -> None:
        self.sf.execute_update(
            """
            UPDATE documents
            SET status='error',
                error_message=%(msg)s
            WHERE id=%(id)s
            """,
            {"id": doc_id, "msg": msg},
        )

    def run(self, limit: int = 1000) -> None:
        docs = self.fetch_cleaned_documents(limit=limit)
        print(f"Found {len(docs)} cleaned docs to chunk (limit={limit})")
        if not docs:
            print("No documents with status='cleaned' to chunk.")
            return

        scanned = skipped = failed = 0
        inserted_chunks = 0

        for d in docs:
            doc_id = str(row_get(d, "id", "ID"))
            ticker = str(row_get(d, "ticker", "TICKER") or "").upper()
            filing_type = str(row_get(d, "filing_type", "FILING_TYPE") or "")
            raw_key = str(row_get(d, "s3_key", "S3_KEY") or "")

            scanned += 1
            print(f"🧩 Chunking: {ticker} {filing_type} id={doc_id}")

            try:
                existing = self.existing_chunk_count(doc_id)
                if existing > 0:
                    skipped += 1
                    self.sf.execute_update(
                        """
                        UPDATE documents
                        SET status='chunked',
                            chunk_count=COALESCE(chunk_count, %(cnt)s),
                            processed_at=COALESCE(processed_at, CURRENT_TIMESTAMP())
                        WHERE id=%(id)s
                        """,
                        {"id": doc_id, "cnt": existing},
                    )
                    print(f"↪️  SKIP (already chunked): id={doc_id} chunks={existing}")
                    continue

                t0 = time.time()

                processed_key = processed_s3_key_from_raw(raw_key)
                doc_text = normalize_ws(self.s3.get_text_gz(key=processed_key))
                if not doc_text:
                    raise ValueError("processed text empty")

                sections = slice_sections(doc_text, filing_type)

                chunk_rows: List[ChunkRow] = []
                chunk_idx = 0

                for sec in sections:
                    sec_label = (sec.section or "Unknown").strip() or "Unknown"
                    sec_chunks = build_chunks_for_section(sec.text)

                    for c in sec_chunks:
                        wc = word_count(c)
                        if wc == 0:
                            continue

                        if wc < MIN_WORDS and len(sec_chunks) > 1:
                            continue

                        if wc > MAX_WORDS:
                            subs = sentence_aware_split(c, MAX_WORDS, OVERLAP_WORDS)
                            for sub in subs:
                                swc = word_count(sub)
                                if swc == 0:
                                    continue
                                s, e = find_char_span(doc_text, sub)
                                chunk_rows.append(
                                    ChunkRow(
                                        id=str(uuid4()),
                                        document_id=doc_id,
                                        chunk_index=chunk_idx,
                                        content=sub,
                                        section=sec_label,
                                        start_char=s,
                                        end_char=e,
                                        word_count=swc,
                                    )
                                )
                                chunk_idx += 1
                            continue

                        s, e = find_char_span(doc_text, c)
                        chunk_rows.append(
                            ChunkRow(
                                id=str(uuid4()),
                                document_id=doc_id,
                                chunk_index=chunk_idx,
                                content=c,
                                section=sec_label,
                                start_char=s,
                                end_char=e,
                                word_count=wc,
                            )
                        )
                        chunk_idx += 1

                if not chunk_rows:
                    raise ValueError("No chunks produced")

                BATCH_SIZE = 150
                for i in range(0, len(chunk_rows), BATCH_SIZE):
                    self.insert_chunks_batch(chunk_rows[i : i + BATCH_SIZE])

                self.mark_chunked(doc_id, chunk_count=len(chunk_rows))

                elapsed = time.time() - t0
                inserted_chunks += len(chunk_rows)
                print(f"✅ Chunked: {ticker} {filing_type} id={doc_id} chunks={len(chunk_rows)} in {elapsed:.1f}s")

            except Exception as e:
                failed += 1
                self.mark_error(doc_id, f"chunk_failed: {type(e).__name__}: {e}")
                print(f"❌ Chunk failed: {ticker} {filing_type} id={doc_id} error={e}")

        print("\n=== CHUNKER SUMMARY ===")
        print(f"Docs scanned: {scanned}")
        print(f"Docs skipped: {skipped}")
        print(f"Docs failed:  {failed}")
        print(f"Chunks inserted: {inserted_chunks}")


def main(limit: int = 1000) -> None:
    DocumentChunkerS3Pipeline().run(limit=limit)
