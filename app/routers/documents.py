from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services.snowflake import SnowflakeService

# Pipelines (must exist in your repo)
from app.pipelines.sec_edgar import collect_for_tickers
from app.pipelines.document_parser import main as parse_main
from app.pipelines.document_text_cleaner import main as clean_main
from app.pipelines.document_chunker_s3 import main as chunk_main

router = APIRouter(prefix="/api/v1/documents", tags=["Documents"])


# -----------------------------
# Utilities
# -----------------------------
def row_get(row: dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def normalize_doc_row(r: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row_get(r, "id", "ID"),
        "company_id": row_get(r, "company_id", "COMPANY_ID"),
        "ticker": row_get(r, "ticker", "TICKER"),
        "filing_type": row_get(r, "filing_type", "FILING_TYPE"),
        "filing_date": row_get(r, "filing_date", "FILING_DATE"),
        "source_url": row_get(r, "source_url", "SOURCE_URL"),
        "local_path": row_get(r, "local_path", "LOCAL_PATH"),
        "s3_key": row_get(r, "s3_key", "S3_KEY"),
        "content_hash": row_get(r, "content_hash", "CONTENT_HASH"),
        "status": row_get(r, "status", "STATUS"),
        "chunk_count": row_get(r, "chunk_count", "CHUNK_COUNT"),
        "error_message": row_get(r, "error_message", "ERROR_MESSAGE"),
        "created_at": row_get(r, "created_at", "CREATED_AT"),
        "processed_at": row_get(r, "processed_at", "PROCESSED_AT"),
    }


# -----------------------------
# Schemas
# -----------------------------
class CollectDocumentsRequest(BaseModel):
    """
    Trigger evidence collection for a company.

    Either provide ticker OR company_id.
    Defaults to running ALL steps: download -> parse -> clean -> chunk
    """
    ticker: Optional[str] = Field(default=None, description="Company ticker (preferred)")
    company_id: Optional[str] = Field(default=None, description="Company UUID (if ticker not provided)")

    filing_types: list[str] = Field(
        default_factory=lambda: ["10-K", "10-Q", "8-K", "DEF 14A"],
        description="Filings to download",
    )
    limit_per_type: int = Field(default=1, ge=1, le=5)

    steps: list[Literal["download", "parse", "clean", "chunk"]] = Field(
        default_factory=lambda: ["download", "parse", "clean", "chunk"],
        description="Which stages to run",
    )

    # pipeline batch limits for stages that work on status queues
    parse_limit: int = Field(default=200, ge=1, le=2000)
    clean_limit: int = Field(default=200, ge=1, le=2000)
    chunk_limit: int = Field(default=200, ge=1, le=2000)


class CollectDocumentsResponse(BaseModel):
    ran_steps: list[str]
    ticker: str
    filing_types: list[str]
    limit_per_type: int
    message: str


class DocumentListResponse(BaseModel):
    items: list[dict[str, Any]]
    limit: int
    offset: int


class ChunkListResponse(BaseModel):
    items: list[dict[str, Any]]
    limit: int
    offset: int


# -----------------------------
# Endpoints
# -----------------------------
@router.post("/collect", response_model=CollectDocumentsResponse)
def collect_documents(payload: CollectDocumentsRequest) -> CollectDocumentsResponse:
    sf = SnowflakeService()

    if not payload.ticker and not payload.company_id:
        raise HTTPException(status_code=400, detail="Provide either ticker or company_id")

    # Resolve ticker if only company_id was provided
    ticker = payload.ticker
    if not ticker:
        rows = sf.execute_query(
            """
            SELECT ticker
            FROM companies
            WHERE id = %(id)s
              AND is_deleted = FALSE
            LIMIT 1
            """,
            {"id": payload.company_id},
        )
        if not rows:
            raise HTTPException(status_code=404, detail="company_id not found")
        ticker = str(row_get(rows[0], "ticker", "TICKER")).upper()

    ticker = str(ticker).upper().strip()
    ran: list[str] = []

    # Step: download (SEC -> S3 -> documents)
    if "download" in payload.steps:
        collect_for_tickers(
            tickers=[ticker],
            filing_types=payload.filing_types,
            limit_per_type=payload.limit_per_type,
        )
        ran.append("download")

    # Step: parse (status='downloaded' -> parsed/..json.gz -> status='parsed')
    if "parse" in payload.steps:
        parse_main(limit=payload.parse_limit)
        ran.append("parse")

    # Step: clean (status='parsed' -> processed/..txt.gz -> status='cleaned')
    if "clean" in payload.steps:
        clean_main(limit=payload.clean_limit)
        ran.append("clean")

    # Step: chunk (status='cleaned' -> document_chunks + status='chunked')
    if "chunk" in payload.steps:
        chunk_main(limit=payload.chunk_limit)
        ran.append("chunk")

    return CollectDocumentsResponse(
        ran_steps=ran,
        ticker=ticker,
        filing_types=payload.filing_types,
        limit_per_type=payload.limit_per_type,
        message="Collection triggered successfully.",
    )


@router.get("", response_model=DocumentListResponse)
def list_documents(
    company_id: Optional[str] = None,
    ticker: Optional[str] = None,
    filing_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> DocumentListResponse:
    sf = SnowflakeService()

    where = ["1=1"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}

    if company_id:
        where.append("company_id = %(company_id)s")
        params["company_id"] = company_id
    if ticker:
        where.append("UPPER(ticker) = %(ticker)s")
        params["ticker"] = ticker.upper()
    if filing_type:
        where.append("filing_type = %(filing_type)s")
        params["filing_type"] = filing_type
    if status:
        where.append("status = %(status)s")
        params["status"] = status

    rows = sf.execute_query(
        f"""
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, s3_key, content_hash,
          status, chunk_count, error_message, created_at, processed_at
        FROM documents
        WHERE {" AND ".join(where)}
        ORDER BY created_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        params,
    )

    return DocumentListResponse(
        items=[normalize_doc_row(r) for r in rows],
        limit=limit,
        offset=offset,
    )


@router.get("/{doc_id}")
def get_document(doc_id: str) -> dict[str, Any]:
    sf = SnowflakeService()
    rows = sf.execute_query(
        """
        SELECT
          id, company_id, ticker, filing_type, filing_date,
          source_url, local_path, s3_key, content_hash,
          status, chunk_count, error_message, created_at, processed_at
        FROM documents
        WHERE id = %(id)s
        LIMIT 1
        """,
        {"id": doc_id},
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Document not found")
    return normalize_doc_row(rows[0])


@router.get("/{doc_id}/chunks", response_model=ChunkListResponse)
def get_document_chunks(
    doc_id: str,
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> ChunkListResponse:
    sf = SnowflakeService()

    rows = sf.execute_query(
        """
        SELECT
          id, document_id, chunk_index, content,
          section, start_char, end_char, word_count
        FROM document_chunks
        WHERE document_id = %(doc_id)s
        ORDER BY chunk_index
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {"doc_id": doc_id, "limit": limit, "offset": offset},
    )

    # Normalize uppercase/lowercase from connector
    def norm_chunk(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": row_get(r, "id", "ID"),
            "document_id": row_get(r, "document_id", "DOCUMENT_ID"),
            "chunk_index": row_get(r, "chunk_index", "CHUNK_INDEX"),
            "content": row_get(r, "content", "CONTENT"),
            "section": row_get(r, "section", "SECTION"),
            "start_char": row_get(r, "start_char", "START_CHAR"),
            "end_char": row_get(r, "end_char", "END_CHAR"),
            "word_count": row_get(r, "word_count", "WORD_COUNT"),
        }

    return ChunkListResponse(items=[norm_chunk(r) for r in rows], limit=limit, offset=offset)