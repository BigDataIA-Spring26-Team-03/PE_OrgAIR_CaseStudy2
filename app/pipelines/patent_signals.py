from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from statistics import mean
from typing import List, Optional, Set

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class PatentSignalInput:
    """
    Represents a single patent-related signal for a company.
    Examples: patent applications, grants, patent text mentions.
    """
    title: str
    abstract: str
    company: str
    url: Optional[str] = None
    published_date: Optional[str] = None  # keep string for now


PATENT_AI_KEYWORDS: Set[str] = {
    "machine learning",
    "deep learning",
    "neural network",
    "transformer",
    "large language model",
    "llm",
    "generative",
    "diffusion",
    "computer vision",
    "natural language processing",
    "nlp",
    "reinforcement learning",
    "rag",
    "vector database",
    "embedding",
}


def _normalize(text: str) -> str:
    t = (text or "").lower()
    t = t.replace("-", " ").replace("_", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def extract_patent_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()
    for kw in PATENT_AI_KEYWORDS:
        if _normalize(kw) in t:
            found.add(_normalize(kw))
    return found


def calculate_patent_innovation_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 innovation intensity based on AI-related patent mentions.
    Simple and deterministic:
      - number of unique AI mentions (cap at 5) contributes 0.7
      - title boost for explicit AI terms contributes 0.3
    """
    base = min(len(mentions) / 5, 1.0) * 0.7

    title_lower = _normalize(title)
    title_boost = 0.3 if any(k in title_lower for k in ["ai", "ml", "machine learning", "llm", "neural"]) else 0.0

    return min(base + title_boost, 1.0)


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|patents|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def patent_inputs_to_signals(company_id: str, items: List[PatentSignalInput]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for item in items:
        text = f"{item.title}\n{item.abstract}"
        mentions = extract_patent_mentions(text)
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
                metadata_json=json.dumps(meta),  # valid JSON
            )
        )

    return signals


def aggregate_patent_signals(company_id: str, patent_signals: List[ExternalSignal]) -> CompanySignalSummary:
    patents_score = int(round(mean(s.score for s in patent_signals))) if patent_signals else 0

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