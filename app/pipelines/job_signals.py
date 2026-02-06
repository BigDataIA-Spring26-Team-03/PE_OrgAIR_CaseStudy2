# app/pipelines/job_signals.py
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from statistics import mean
from typing import Dict, List, Optional, Set

from jobspy import scrape_jobs

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


class SkillCategory(str, Enum):
    ML_ENGINEERING = "ml_engineering"
    DATA_SCIENCE = "data_science"
    AI_INFRASTRUCTURE = "ai_infrastructure"
    AI_PRODUCT = "ai_product"
    AI_STRATEGY = "ai_strategy"


AI_SKILLS: Dict[SkillCategory, Set[str]] = {
    SkillCategory.ML_ENGINEERING: {
        "pytorch",
        "tensorflow",
        "keras",
        "mlops",
        "deep learning",
        "transformers",
        "llm",
        "fine-tuning",
        "model training",
    },
    SkillCategory.DATA_SCIENCE: {
        "data science",
        "statistics",
        "feature engineering",
        "scikit-learn",
        "sklearn",
        "xgboost",
        "lightgbm",
        "numpy",
        "pandas",
    },
    SkillCategory.AI_INFRASTRUCTURE: {
        "aws",
        "azure",
        "gcp",
        "docker",
        "kubernetes",
        "snowflake",
        "databricks",
        "spark",
        "airflow",
        "vector database",
        "faiss",
        "pinecone",
    },
    SkillCategory.AI_PRODUCT: {
        "prompt engineering",
        "rag",
        "product analytics",
        "experimentation",
        "a/b testing",
        "recommendation",
        "personalization",
    },
    SkillCategory.AI_STRATEGY: {
        "ai strategy",
        "governance",
        "responsible ai",
        "model risk",
        "compliance",
        "enterprise ai",
        "roadmap",
    },
}

SENIORITY_KEYWORDS = {
    "intern": ["intern", "internship", "co-op", "coop"],
    "junior": ["junior", "entry", "associate", "new grad", "graduate"],
    "mid": ["engineer", "analyst", "developer", "scientist"],  # fallback bucket
    "senior": ["senior", "sr", "lead", "principal", "staff"],
    "manager": ["manager", "head", "director", "vp", "chief"],
}


@dataclass(frozen=True)
class JobPosting:
    title: str
    description: str
    company: str
    url: Optional[str] = None
    posted_date: Optional[str] = None


def classify_seniority(title: str) -> str:
    t = (title or "").lower()
    for level, kws in SENIORITY_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return level
    return "mid"


def extract_ai_skills(text: str) -> Set[str]:
    text_lower = (text or "").lower()
    found: Set[str] = set()
    for _, skills_set in AI_SKILLS.items():
        for skill in skills_set:
            if skill in text_lower:
                found.add(skill)
    return found


def calculate_ai_relevance_score(skills: Set[str], title: str) -> float:
    base_score = min(len(skills) / 5, 1.0) * 0.6
    title_lower = (title or "").lower()
    title_keywords = [
        "ai",
        "ml",
        "machine learning",
        "data scientist",
        "mlops",
        "artificial intelligence",
    ]
    title_boost = 0.4 if any(kw in title_lower for kw in title_keywords) else 0.0
    return min(base_score + title_boost, 1.0)


def _signal_id(company_id: str, category: SignalCategory, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|{category.value}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def _norm_company(s: str) -> str:
    """
    Normalize company strings for rough matching.
    Example: 'Walmart Inc.' -> 'walmart'
    """
    x = (s or "").lower().strip()
    x = re.sub(r"[^a-z0-9 ]+", " ", x)
    x = re.sub(
        r"\b(inc|incorporated|corp|corporation|llc|ltd|limited|co|company|plc)\b",
        " ",
        x,
    )
    x = re.sub(r"\s+", " ", x).strip()
    return x


def job_postings_to_signals(company_id: str, jobs: List[JobPosting]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for job in jobs:
        skills = extract_ai_skills(job.description)
        seniority = classify_seniority(job.title)
        relevance_0_1 = calculate_ai_relevance_score(skills, job.title)
        score_0_100 = int(round(relevance_0_1 * 100))

        meta = {
            "company": job.company,
            "seniority": seniority,
            "skills": sorted(list(skills)),
            "posted_date": job.posted_date,
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, SignalCategory.jobs, job.title, job.url),
                company_id=company_id,
                category=SignalCategory.jobs,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=job.title,
                url=job.url,
                metadata_json=json.dumps(meta, default=str),
            )
        )

    return signals


def aggregate_job_signals(company_id: str, job_signals: list[ExternalSignal]) -> CompanySignalSummary:
    if not job_signals:
        jobs_score = 0
    else:
        jobs_score = int(round(mean(s.score for s in job_signals)))

    # other pipelines fill these later in orchestrator summary builder
    tech_score = 0
    patents_score = 0
    leadership_score = 0

    composite_score = int(
        round(0.30 * jobs_score + 0.25 * patents_score + 0.25 * tech_score + 0.20 * leadership_score)
    )

    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=jobs_score,
        tech_score=tech_score,
        patents_score=patents_score,
        leadership_score=leadership_score,
        composite_score=composite_score,
        last_updated_at=datetime.utcnow(),
    )


def scrape_job_postings(
    search_query: str,
    sources: list[str] = ["linkedin", "indeed", "glassdoor"],
    location: str = "United States",
    max_results_per_source: int = 25,
    hours_old: int = 24 * 30,
    target_company_name: Optional[str] = None,
    target_company_aliases: Optional[list[str]] = None,  # ✅ NEW
) -> list[JobPosting]:
    """
    Scrape job postings using JobSpy and return JobPosting objects.

    If target_company_name/aliases are provided:
      1) BOOST recall by adding (a best alias) into the search query
      2) FILTER results to ANY alias (contains OR normalized-equality)
    """
    # -----------------------------
    # Build alias list
    # -----------------------------
    aliases: list[str] = []
    if target_company_name:
        aliases.append(target_company_name)
    if target_company_aliases:
        aliases.extend([a for a in target_company_aliases if a])

    aliases = [a.strip() for a in aliases if a and a.strip()]
    alias_raws = [a.lower() for a in aliases]
    alias_norms = [_norm_company(a) for a in aliases]

    # -----------------------------
    # Boost recall (query)
    # Prefer short alias like ticker ("ADP") if present; else use company name
    # -----------------------------
    effective_query = search_query
    if aliases:
        best_alias = min(aliases, key=len)  # short tends to work well for recall (tickers)
        effective_query = f'{search_query} "{best_alias}"'

    df = scrape_jobs(
        site_name=sources,
        search_term=effective_query,
        location=location,
        results_wanted=max_results_per_source * len(sources),
        hours_old=hours_old,
        linkedin_fetch_description=True,
    )

    if df is None or df.empty:
        return []

    # -----------------------------
    # Filter to company (ANY alias)
    # -----------------------------
    if aliases and "company" in df.columns:

        def is_match(company_val: object) -> bool:
            c = str(company_val or "")
            c_lower = c.lower()
            c_norm = _norm_company(c)

            # 1) contains match for any alias ("adp" in "ADP", "walmart" in "Walmart Inc.")
            for a in alias_raws:
                if a and a in c_lower:
                    return True

            # 2) normalized equality fallback
            for n in alias_norms:
                if n and n == c_norm:
                    return True

            return False

        df = df[df["company"].apply(is_match)]
        if df.empty:
            return []

    jobs: list[JobPosting] = []
    for _, row in df.iterrows():
        jobs.append(
            JobPosting(
                title=str(row.get("title", "")),
                company=str(row.get("company", "Unknown")),
                description=str(row.get("description", "")),
                url=str(row.get("job_url", "")),
                posted_date=str(row.get("date_posted", "")),
            )
        )

    return jobs