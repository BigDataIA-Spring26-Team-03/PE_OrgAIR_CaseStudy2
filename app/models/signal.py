from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class SignalCategory(str, Enum):
    jobs = "jobs"
    tech = "tech"
    patents = "patents"
    leadership = "leadership"


class SignalSource(str, Enum):
    # keep generic so we can map any collector later
    external = "external"
    internal = "internal"


class ExternalSignal(BaseModel):
    # matches external_signals table columns
    id: str
    company_id: str

    category: SignalCategory
    source: SignalSource = SignalSource.external

    signal_date: datetime
    score: int = Field(..., ge=0, le=100)

    title: Optional[str] = Field(default=None, max_length=300)
    url: Optional[str] = Field(default=None, max_length=500)
    metadata_json: Optional[str] = None

    created_at: Optional[datetime] = None


class CompanySignalSummary(BaseModel):
    # matches company_signal_summaries table columns
    company_id: str

    jobs_score: int = Field(..., ge=0, le=100)
    tech_score: int = Field(..., ge=0, le=100)
    patents_score: int = Field(..., ge=0, le=100)
    leadership_score: int = Field(default=0, ge=0, le=100)


    composite_score: int = Field(..., ge=0, le=100)

    last_updated_at: Optional[datetime] = None
    