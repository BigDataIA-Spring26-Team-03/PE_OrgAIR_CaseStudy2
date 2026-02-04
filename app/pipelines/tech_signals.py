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


# --- Keyword sets (keep lightweight, but include what our lab examples use) ---

CORE_AI_TECH: Set[str] = {
    "openai",
    "chatgpt",
    "gpt",
    "llm",
    "transformers",
    "rag",
    "vector database",
    "vector db",
    "pytorch",
    "tensorflow",
    "keras",
    "hugging face",
    "huggingface",
    "langchain",
    "llamaindex",
}

DATA_PLATFORM_TECH: Set[str] = {
    "snowflake",
    "databricks",
    "spark",
    "airflow",
    "kafka",
    "dbt",
    "delta lake",
    "s3",
    "adls",
    "bigquery",
    "redshift",
}

CLOUD_AI_SERVICES: Set[str] = {
    "aws sagemaker",
    "sagemaker",
    "bedrock",
    "azure openai",
    "azure ml",
    "vertex ai",
    "google cloud ai",
    "amazon comprehend",
}

# IMPORTANT: include common “tech adoption” signals used in examples
GENERAL_TECH: Set[str] = {
    "azure",
    "aws",
    "gcp",
    "kubernetes",
    "k8s",
    "docker",
    "github",
    "open source",
    "opensource",
}


def _normalize(text: str) -> str:
    """
    Normalize text for robust matching:
    - lowercase
    - replace common separators with spaces
    - collapse repeated whitespace
    """
    t = (text or "").lower()
    t = t.replace("-", " ").replace("_", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _contains_keyword(text_norm: str, kw: str) -> bool:
    """
    For very short tokens (ai/ml/llm/gpt) do word-boundary match.
    For multiword/normal tokens, do substring match on normalized text.
    """
    kw_norm = _normalize(kw)

    # word-boundary for short ambiguous tokens
    if kw_norm in {"ai", "ml", "llm", "gpt"}:
        return re.search(rf"\b{re.escape(kw_norm)}\b", text_norm) is not None

    return kw_norm in text_norm


def extract_tech_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()

    all_keywords = CORE_AI_TECH | DATA_PLATFORM_TECH | CLOUD_AI_SERVICES | GENERAL_TECH
    for kw in all_keywords:
        if _contains_keyword(t, kw):
            # store the keyword in a consistent normalized form
            found.add(_normalize(kw))

    return found


def calculate_tech_adoption_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 based on number and type of tech mentions.
    Heavier weight for core AI tech; moderate for data platforms; some for cloud AI services.
    """
    if not mentions:
        return 0.0

    # normalize keyword sets once for consistent membership checks
    core_set = {_normalize(x) for x in CORE_AI_TECH}
    data_set = {_normalize(x) for x in DATA_PLATFORM_TECH}
    cloud_set = {_normalize(x) for x in CLOUD_AI_SERVICES}
    general_set = {_normalize(x) for x in GENERAL_TECH}

    core_hits = sum(1 for m in mentions if m in core_set)
    data_hits = sum(1 for m in mentions if m in data_set)
    cloud_hits = sum(1 for m in mentions if m in cloud_set)
    general_hits = sum(1 for m in mentions if m in general_set)

    # weighted sum with caps (keep simple + stable)
    score = (
        min(core_hits, 3) * 0.25 +
        min(data_hits, 3) * 0.12 +
        min(cloud_hits, 2) * 0.13 +
        min(general_hits, 3) * 0.05
    )

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
                metadata_json=json.dumps(meta),  # valid JSON for Snowflake PARSE_JSON later
            )
        )

    return signals


def aggregate_tech_signals(company_id: str, tech_signals: List[ExternalSignal]) -> CompanySignalSummary:
    tech_score = int(round(mean(s.score for s in tech_signals))) if tech_signals else 0

    # jobs & patents filled by other pipelines later
    jobs_score = 0
    patents_score = 0

    composite_score = int(round(0.5 * jobs_score + 0.3 * tech_score + 0.2 * patents_score))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        composite_score=composite_score,
    )

def scrape_tech_signal_inputs_mock(company: str) -> List[TechSignalInput]:
    """
    MOCK tech signal "scraper".
    Returns a few TechSignalInput items as if they were scraped from blogs/GitHub/news.
    """
    today = datetime.utcnow().strftime("%Y-%m-%d")
    return [
        TechSignalInput(
            title="AI Platform Launch",
            description="We announced an LLM agent workflow with RAG using Azure OpenAI and Kubernetes.",
            company=company,
            url="https://example.com/ai-platform-launch",
            observed_date=today,
        ),
        TechSignalInput(
            title="Open source release",
            description="We released an open source repo for evaluation pipelines, using LangChain + vector database patterns.",
            company=company,
            url="https://github.com/example/eval-pipelines",
            observed_date=today,
        ),
    ]