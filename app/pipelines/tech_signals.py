from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from typing import List, Optional, Set
from statistics import mean
from app.models.signal import CompanySignalSummary

from app.models.signal import ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class TechSignalInput:
    """
    Represents a single tech-stack signal for a company.
    Examples: detected technologies, stack mentions, vendor tools, platform usage.
    """
    title: str
    description: str
    company: str
    url: Optional[str] = None
    observed_date: Optional[str] = None  # keep string for now


CORE_AI_TECH: Set[str] = {
    "openai", "chatgpt", "gpt", "llm", "transformers", "rag", "vector database",
    "pytorch", "tensorflow", "keras", "hugging face", "langchain", "llamaindex",
}

DATA_PLATFORM_TECH: Set[str] = {
    "snowflake", "databricks", "spark", "airflow", "kafka", "dbt", "delta lake",
    "s3", "adls", "bigquery", "redshift",
}

CLOUD_AI_SERVICES: Set[str] = {
    "aws sagemaker", "bedrock", "azure openai", "azure ml", "vertex ai",
    "google cloud ai", "amazon comprehend",
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def extract_tech_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()

    for kw in CORE_AI_TECH | DATA_PLATFORM_TECH | CLOUD_AI_SERVICES:
        if kw in t:
            found.add(kw)

    return found


def calculate_tech_adoption_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 based on number and type of tech mentions.
    Heavier weight for core AI tech; moderate for data platforms; some for cloud AI services.
    """
    if not mentions:
        return 0.0

    core_hits = sum(1 for m in mentions if m in CORE_AI_TECH)
    data_hits = sum(1 for m in mentions if m in DATA_PLATFORM_TECH)
    cloud_hits = sum(1 for m in mentions if m in CLOUD_AI_SERVICES)

    # weighted sum with caps
    score = min(core_hits, 3) * 0.25 + min(data_hits, 3) * 0.12 + min(cloud_hits, 2) * 0.13

    # title boost (if signal explicitly sounds AI-related)
    title_lower = _normalize(title)
    title_boost = 0.15 if any(k in title_lower for k in ["ai", "ml", "machine learning", "llm", "genai"]) else 0.0

    return min(score + title_boost, 1.0)


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|tech|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def tech_inputs_to_signals(company_id: str, items: List[TechSignalInput]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for item in items:
        mentions = extract_tech_mentions(item.description)
        score_0_1 = calculate_tech_adoption_score(mentions, item.title)
        score_0_100 = int(round(score_0_1 * 100))

        meta = {
            "company": item.company,
            "mentions": sorted(list(mentions)),
            "observed_date": item.observed_date,
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, item.title, item.url),
                company_id=company_id,
                category=SignalCategory.tech,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=item.title,
                url=item.url,
                metadata_json=str(meta),
            )
        )

    return signals

def aggregate_tech_signals(company_id: str, tech_signals: list[ExternalSignal]) -> CompanySignalSummary:
    if not tech_signals:
        tech_score = 0
    else:
        tech_score = int(round(mean(s.score for s in tech_signals)))

    # jobs & patents filled by other pipelines later
    jobs_score = 0
    patents_score = 0

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