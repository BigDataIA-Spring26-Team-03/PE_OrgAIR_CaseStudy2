from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import List, Optional, Set
from statistics import mean
from app.models.signal import CompanySignalSummary


from app.models.signal import ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class PatentSignalInput:
    """
    Represents a single patent-related signal.
    Could come from USPTO, Google Patents, news, or curated datasets.
    """
    title: str
    abstract: str
    company: str
    url: Optional[str] = None
    filing_date: Optional[str] = None  # keep as string for now


AI_PATENT_KEYWORDS: Set[str] = {
    "artificial intelligence", "machine learning", "deep learning", "neural network",
    "transformer", "large language model", "llm", "generative", "foundation model",
    "natural language processing", "nlp", "computer vision", "reinforcement learning",
    "recommendation", "prediction", "classification",
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def extract_patent_ai_terms(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()
    for kw in AI_PATENT_KEYWORDS:
        if kw in t:
            found.add(kw)
    return found


def calculate_patent_strength_score(terms: Set[str], title: str) -> float:
    """
    Score 0..1 based on AI-related terms.
    Simple but stable: more AI terms => stronger AI IP signal.
    """
    if not terms:
        return 0.0

    # base by term count (cap at 5 terms)
    base = min(len(terms), 5) / 5  # 0..1

    # title boost if explicitly AI-ish
    title_lower = _normalize(title)
    title_boost = 0.15 if any(k in title_lower for k in ["ai", "ml", "machine learning", "generative", "llm"]) else 0.0

    return min(base * 0.85 + title_boost, 1.0)


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|patents|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def patent_inputs_to_signals(company_id: str, items: List[PatentSignalInput]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for item in items:
        terms = extract_patent_ai_terms(f"{item.title}\n{item.abstract}")
        score_0_1 = calculate_patent_strength_score(terms, item.title)
        score_0_100 = int(round(score_0_1 * 100))

        meta = {
            "company": item.company,
            "ai_terms": sorted(list(terms)),
            "filing_date": item.filing_date,
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
                metadata_json=str(meta),
            )
        )

    return signals

def aggregate_patent_signals(company_id: str, patent_signals: list[ExternalSignal]) -> CompanySignalSummary:
    if not patent_signals:
        patents_score = 0
    else:
        patents_score = int(round(mean(s.score for s in patent_signals)))

    # jobs & tech filled by other pipelines later
    jobs_score = 0
    tech_score = 0

    composite_score = int(
        round(
            0.5 * jobs_score +
            0.3 * tech_score +
            0.2 * patents_score
        )
    )

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        composite_score=composite_score,
    )