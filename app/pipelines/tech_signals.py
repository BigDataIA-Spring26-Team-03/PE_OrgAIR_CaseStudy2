from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from statistics import mean
from typing import List, Optional, Set, Dict, Iterable

import requests
from bs4 import BeautifulSoup

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


@dataclass(frozen=True)
class TechSignalInput:
    """
    Represents a single tech-stack signal for a company.
    For Digital Presence we collect from a company's public website (domain).
    """
    title: str
    description: str
    company: str
    url: Optional[str] = None
    observed_date: Optional[str] = None  # keep string for now


# -----------------------------
# Keyword dictionaries (expandable)
# -----------------------------
CORE_AI_TECH: Set[str] = {
    "openai", "chatgpt", "gpt", "llm", "transformers", "rag", "vector database",
    "pytorch", "tensorflow", "keras", "hugging face", "langchain", "llamaindex",
    "embedding", "embeddings", "genai", "generative ai",
}

DATA_PLATFORM_TECH: Set[str] = {
    "snowflake", "databricks", "spark", "airflow", "kafka", "dbt", "delta lake",
    "s3", "adls", "bigquery", "redshift",
}

CLOUD_AI_SERVICES: Set[str] = {
    "aws sagemaker", "bedrock", "azure openai", "azure ml", "vertex ai",
    "google cloud ai", "amazon comprehend",
}

WEB_STACK_TECH: Set[str] = {
    "react", "next.js", "nextjs", "angular", "vue", "svelte",
    "node.js", "nodejs", "express", "django", "flask", "fastapi",
    "kubernetes", "docker", "terraform",
    "cloudflare", "akamai",
    "segment", "amplitude", "mixpanel",
    "google analytics", "gtag", "gtm", "tag manager",
    "stripe", "paypal",
}


def _normalize(text: str) -> str:
    return (text or "").lower()


def _signal_id(company_id: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|tech|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def extract_tech_mentions(text: str) -> Set[str]:
    t = _normalize(text)
    found: Set[str] = set()

    for kw in (CORE_AI_TECH | DATA_PLATFORM_TECH | CLOUD_AI_SERVICES | WEB_STACK_TECH):
        if kw in t:
            found.add(kw)

    return found


def calculate_tech_adoption_score(mentions: Set[str], title: str) -> float:
    """
    Score 0..1 based on number and type of tech mentions.
    """
    if not mentions:
        return 0.0

    core_hits = sum(1 for m in mentions if m in CORE_AI_TECH)
    data_hits = sum(1 for m in mentions if m in DATA_PLATFORM_TECH)
    cloud_hits = sum(1 for m in mentions if m in CLOUD_AI_SERVICES)
    web_hits = sum(1 for m in mentions if m in WEB_STACK_TECH)

    # weighted sum with caps (tunable)
    score = (
        min(core_hits, 3) * 0.22 +
        min(data_hits, 3) * 0.12 +
        min(cloud_hits, 2) * 0.14 +
        min(web_hits, 4) * 0.08
    )

    title_lower = _normalize(title)
    title_boost = 0.10 if any(k in title_lower for k in ["ai", "ml", "machine learning", "llm", "genai", "platform"]) else 0.0

    return min(score + title_boost, 1.0)


# -----------------------------
# REAL COLLECTION: scrape company website tech
# -----------------------------
def _ensure_url(domain_or_url: str) -> str:
    x = (domain_or_url or "").strip()
    if not x:
        return ""
    if x.startswith("http://") or x.startswith("https://"):
        return x
    return f"https://{x}"


def _fetch_html(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; PE_OrgAIR/1.0; +https://example.com)"
    }
    r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    r.raise_for_status()
    return r.text or ""


def _extract_visible_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Remove scripts/styles
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(" ", strip=True)
    # compress whitespace
    return re.sub(r"\s+", " ", text)


def _extract_script_srcs(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    srcs = []
    for s in soup.find_all("script"):
        src = s.get("src")
        if src:
            srcs.append(str(src))
    return srcs


def scrape_tech_signal_inputs(
    company: str,
    company_domain_or_url: str,
) -> List[TechSignalInput]:
    """
    REAL Digital Presence collector.
    Looks at company homepage HTML and detects tech keywords from:
      - visible text
      - script src URLs (often reveal analytics/CDNs/frameworks)
    """
    url = _ensure_url(company_domain_or_url)
    if not url:
        return []

    try:
        html = _fetch_html(url)
    except Exception as e:
        # fail open: return empty list if site blocks/timeout
        return [
            TechSignalInput(
                title="Digital presence scan failed",
                description=f"Failed to fetch {url}: {e}",
                company=company,
                url=url,
                observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
            )
        ]

    visible_text = _extract_visible_text(html)
    script_srcs = " ".join(_extract_script_srcs(html))

    # Combine sources
    combined = f"{visible_text} {script_srcs}"

    mentions = extract_tech_mentions(combined)

    # If we detected nothing, still create one signal item as evidence of scan
    desc = (
        f"Scanned {url} for technology indicators. "
        f"Detected {len(mentions)} technologies."
    )
    if mentions:
        desc += f" Mentions: {', '.join(sorted(mentions))}."

    return [
        TechSignalInput(
            title="Digital presence technology scan",
            description=desc,
            company=company,
            url=url,
            observed_date=datetime.utcnow().strftime("%Y-%m-%d"),
        )
    ]


# -----------------------------
# Convert + Aggregate
# -----------------------------
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
            "url": item.url,
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
                metadata_json=json.dumps(meta),
            )
        )

    return signals


def aggregate_tech_signals(company_id: str, tech_signals: List[ExternalSignal]) -> CompanySignalSummary:
    if not tech_signals:
        tech_score = 0
    else:
        tech_score = int(round(mean(s.score for s in tech_signals)))

    # other pipelines fill these later
    jobs_score = 0
    patents_score = 0
    leadership_score = 0

    composite_score = int(round(
        0.30 * jobs_score +
        0.25 * patents_score +
        0.25 * tech_score +
        0.20 * leadership_score
    ))

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        leadership_score=leadership_score,
        composite_score=composite_score,
        last_updated_at=datetime.utcnow(),
    )