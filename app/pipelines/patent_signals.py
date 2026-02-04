from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from statistics import mean
from typing import List, Optional, Set

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class PatentSignalInput:
    """
    Represents a single patent / innovation signal for a company.
    """
    title: str
    abstract: str
    company: str
    url: Optional[str] = None
    published_date: Optional[str] = None  # keep string for now


PATENT_KEYWORDS: Set[str] = {
    "patent",
    "invention",
    "novel",
    "embeddings",
    "embedding",
    "large language model",
    "llm",
    "transformer",
    "transformers",
    "neural network",
    "deep learning",
    "nlp",
    "generative",
    "foundation model",
    "rag",
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def extract_patent_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()
    for kw in PATENT_KEYWORDS:
        if kw in t:
            found.add(kw)
    return found


def calculate_patent_innovation_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 based on number of patent/AI innovation mentions + a small title boost.
    """
    if not mentions:
        return 0.0

    # cap keyword hits so it doesn't explode
    hit_score = min(len(mentions), 6) / 6.0  # 0..1
    base = 0.85 * hit_score

    title_lower = _normalize(title)
    title_boost = 0.15 if any(k in title_lower for k in ["patent", "transformer", "llm", "neural", "generative"]) else 0.0
    return min(base + title_boost, 1.0)


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|patents|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def patent_inputs_to_signals(company_id: str, items: List[PatentSignalInput]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for item in items:
        mentions = extract_patent_mentions(item.abstract)
        score_0_1 = calculate_patent_innovation_score(mentions, item.title)
        score_0_100 = int(round(score_0_1 * 100))

        meta = {
            "company": item.company,
            "mentions": sorted(list(mentions)),
            "published_date": item.published_date,
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, item.title, item.url),
                company_id=company_id,
                category=SignalCategory.patents,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=item.title,
                url=item.url,
                metadata_json=json.dumps(meta),  # IMPORTANT: valid JSON
            )
        )

    return signals


def aggregate_patent_signals(company_id: str, patent_signals: List[ExternalSignal]) -> CompanySignalSummary:
    if not patent_signals:
        patents_score = 0
    else:
        patents_score = int(round(mean(s.score for s in patent_signals)))

    # jobs & tech filled by other pipelines later
    jobs_score = 0
    tech_score = 0

    composite_score = int(round(0.5 * jobs_score + 0.3 * tech_score + 0.2 * patents_score))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        composite_score=composite_score,
    )


def scrape_patent_signal_inputs_mock(company: str = "TestCo") -> List[PatentSignalInput]:
    """
    MOCK ONLY (no real scraping yet). Safe constructor: uses `abstract=` (NOT description).
    """
    return [
        PatentSignalInput(
            title="Neural network model for generative text",
            abstract="A large language model uses embeddings and transformer layers for NLP tasks",
            company=company,
            url="https://example.com/patent1",
            published_date="2025-11-10",
        )
    ]