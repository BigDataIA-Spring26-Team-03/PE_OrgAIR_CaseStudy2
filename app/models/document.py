from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class DocumentStatus(str, Enum):
    pending = "pending"
    downloaded = "downloaded"
    parsed = "parsed"
    chunked = "chunked"
    indexed = "indexed"
    failed = "failed"


class DocumentRecord(BaseModel):
    # matches documents table columns
    id: str
    company_id: str
    ticker: str = Field(..., max_length=10)
    filing_type: str = Field(..., max_length=20)
    filing_date: date

    source_url: Optional[str] = Field(default=None, max_length=500)
    local_path: Optional[str] = Field(default=None, max_length=500)
    s3_key: Optional[str] = Field(default=None, max_length=500)

    content_hash: Optional[str] = Field(default=None, max_length=64)
    word_count: Optional[int] = None
    chunk_count: Optional[int] = None

    status: DocumentStatus = DocumentStatus.pending
    error_message: Optional[str] = Field(default=None, max_length=1000)

    created_at: Optional[datetime] = None
    processed_at: Optional[datetime] = None