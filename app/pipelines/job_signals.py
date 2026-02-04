from __future__ import annotations
import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from statistics import mean
from app.models.signal import CompanySignalSummary
from typing import Dict, List, Optional, Set
from jobspy import scrape_jobs
import pandas as pd

from app.models.signal import ExternalSignal, SignalCategory, SignalSource


class SkillCategory(str, Enum):
    ML_ENGINEERING = "ml_engineering"
    DATA_SCIENCE = "data_science"
    AI_INFRASTRUCTURE = "ai_infrastructure"
    AI_PRODUCT = "ai_product"
    AI_STRATEGY = "ai_strategy"


AI_SKILLS: Dict[SkillCategory, Set[str]] = {
    SkillCategory.ML_ENGINEERING: {
        "pytorch", "tensorflow", "keras", "mlops", "deep learning", "transformers",
        "llm", "fine-tuning", "model training"
    },
    SkillCategory.DATA_SCIENCE: {
        "data science", "statistics", "feature engineering", "scikit-learn", "sklearn",
        "xgboost", "lightgbm", "numpy", "pandas"
    },
    SkillCategory.AI_INFRASTRUCTURE: {
        "aws", "azure", "gcp", "docker", "kubernetes", "snowflake", "databricks",
        "spark", "airflow", "vector database", "faiss", "pinecone"
    },
    SkillCategory.AI_PRODUCT: {
        "prompt engineering", "rag", "product analytics", "experimentation", "a/b testing",
        "recommendation", "personalization"
    },
    SkillCategory.AI_STRATEGY: {
        "ai strategy", "governance", "responsible ai", "model risk", "compliance",
        "enterprise ai", "roadmap"
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
    posted_date: Optional[str] = None  # keep string for now (we can normalize later)


def classify_seniority(title: str) -> str:
    t = title.lower()
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
    # 0..1 score
    base_score = min(len(skills) / 5, 1.0) * 0.6

    title_lower = (title or "").lower()
    title_keywords = ["ai", "ml", "machine learning", "data scientist", "mlops", "artificial intelligence"]
    title_boost = 0.4 if any(kw in title_lower for kw in title_keywords) else 0.0

    return min(base_score + title_boost, 1.0)


def _signal_id(company_id: str, category: SignalCategory, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|{category.value}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def job_postings_to_signals(company_id: str, jobs: List[JobPosting]) -> List[ExternalSignal]:
    signals: List[ExternalSignal] = []
    now = datetime.utcnow()

    for job in jobs:
        skills = extract_ai_skills(job.description)
        seniority = classify_seniority(job.title)
        relevance_0_1 = calculate_ai_relevance_score(skills, job.title)

        score_0_100 = int(round(relevance_0_1 * 100))

        # store useful context in metadata_json (simple stringified dict for now)
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
    
def aggregate_job_signals(
    company_id: str,
    job_signals: list[ExternalSignal],
) -> CompanySignalSummary:
    """
    Aggregate job-based ExternalSignals into a company-level summary.
    """

    if not job_signals:
        jobs_score = 0
    else:
        jobs_score = int(round(mean(s.score for s in job_signals)))

    # tech & patents will be filled by other pipelines later
    tech_score = 0
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

def scrape_job_postings(
    search_query: str,
    sources: list[str] = ["linkedin", "indeed", "glassdoor"],
    location: str = "United States",
    max_results_per_source: int = 25,
    hours_old: int = 24 * 30,
) -> list[JobPosting]:
    """
    Scrape job postings using JobSpy and return JobPosting objects.
    """

    df = scrape_jobs(
        site_name=sources,
        search_term=search_query,
        location=location,
        results_wanted=max_results_per_source * len(sources),
        hours_old=hours_old,
        linkedin_fetch_description=True,
    )

    jobs: list[JobPosting] = []

    if df is None or df.empty:
        return jobs

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