from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from hashlib import sha256
from statistics import mean
from typing import List, Optional

from app.models.signal import CompanySignalSummary, ExternalSignal, SignalCategory, SignalSource


class AIBackgroundType(str, Enum):
    CHIEF_AI_OFFICER = "CHIEF_AI_OFFICER"          # 1.0
    AI_COMPANY_VETERAN = "AI_COMPANY_VETERAN"      # 0.9 (Google AI, Meta FAIR, OpenAI)
    PHD_AI_ML = "PHD_AI_ML"                        # 0.8
    ML_PUBLICATIONS = "ML_PUBLICATIONS"            # 0.7
    AI_PATENTS = "AI_PATENTS"                      # 0.6
    AI_BOARD_MEMBER = "AI_BOARD_MEMBER"            # 0.5
    AI_CERTIFICATION = "AI_CERTIFICATION"          # 0.3
    AI_KEYWORDS_ONLY = "AI_KEYWORDS_ONLY"          # 0.1


AI_INDICATOR_SCORES: dict[AIBackgroundType, float] = {
    AIBackgroundType.CHIEF_AI_OFFICER: 1.0,
    AIBackgroundType.AI_COMPANY_VETERAN: 0.9,
    AIBackgroundType.PHD_AI_ML: 0.8,
    AIBackgroundType.ML_PUBLICATIONS: 0.7,
    AIBackgroundType.AI_PATENTS: 0.6,
    AIBackgroundType.AI_BOARD_MEMBER: 0.5,
    AIBackgroundType.AI_CERTIFICATION: 0.3,
    AIBackgroundType.AI_KEYWORDS_ONLY: 0.1,
}


ROLE_WEIGHTS: dict[str, float] = {
    "ceo": 1.0,
    "cto": 0.9,
    "cdo": 0.85,
    "cai": 1.0,   # Chief AI Officer shorthand
    "vp": 0.7,
}


@dataclass(frozen=True)
class LeadershipProfile:
    name: str
    title: str
    company: str
    ai_indicators: List[AIBackgroundType]
    url: Optional[str] = None
    observed_date: Optional[str] = None


def _normalize(s: str) -> str:
    return (s or "").strip().lower()


def _role_weight(title: str) -> float:
    t = _normalize(title)

    # quick heuristics
    if "chief ai officer" in t or "cai" in t:
        return ROLE_WEIGHTS["cai"]
    if t.startswith("ceo") or "chief executive" in t:
        return ROLE_WEIGHTS["ceo"]
    if t.startswith("cto") or "chief technology" in t:
        return ROLE_WEIGHTS["cto"]
    if t.startswith("cdo") or "chief data" in t:
        return ROLE_WEIGHTS["cdo"]
    if t.startswith("vp") or "vice president" in t:
        return ROLE_WEIGHTS["vp"]

    # default lower weight if unknown role
    return 0.5


def _max_indicator_score(indicators: List[AIBackgroundType]) -> float:
    if not indicators:
        return 0.0
    return max(AI_INDICATOR_SCORES.get(i, 0.0) for i in indicators)


def calculate_leadership_score_0_1(executives: List[LeadershipProfile]) -> float:
    """
    LeadershipScore = avg_i ( role_weight_i * max(ai_indicators_i) )
    capped to 1.0
    """
    if not executives:
        return 0.0

    weighted_sum = 0.0
    for e in executives:
        weighted_sum += _role_weight(e.title) * _max_indicator_score(e.ai_indicators)

    return min(weighted_sum / len(executives), 1.0)


def _signal_id(company_id: str, name: str, title: str, url: Optional[str]) -> str:
    raw = f"{company_id}|leadership|{name}|{title}|{url or ''}"
    return sha256(raw.encode("utf-8")).hexdigest()


def leadership_profiles_to_signals(company_id: str, executives: List[LeadershipProfile]) -> List[ExternalSignal]:
    """
    We create 1 signal per executive (so you can inspect metadata per profile),
    and aggregation computes the company-level leadership score.
    """
    now = datetime.utcnow()
    signals: List[ExternalSignal] = []

    for e in executives:
        ai_score = _max_indicator_score(e.ai_indicators)          # 0..1
        role_w = _role_weight(e.title)                            # 0..1 (ish)
        # per-exec score: max-indicator only (simple + interpretable)
        score_0_100 = int(round(ai_score * 100))

        meta = {
            "company": e.company,
            "executive_name": e.name,
            "executive_title": e.title,
            "ai_indicators": [i.value for i in e.ai_indicators],
            "max_indicator_score": ai_score,
            "role_weight": role_w,
            "observed_date": e.observed_date,
        }

        signals.append(
            ExternalSignal(
                id=_signal_id(company_id, e.name, e.title, e.url),
                company_id=company_id,
                category=SignalCategory.leadership,
                source=SignalSource.external,
                signal_date=now,
                score=score_0_100,
                title=f"{e.name} — {e.title}",
                url=e.url,
                metadata_json=json.dumps(meta),
            )
        )

    return signals


def aggregate_leadership_signals(company_id: str, leadership_signals: List[ExternalSignal]) -> CompanySignalSummary:
    """
    Returns a CompanySignalSummary object, but only leadership_score is meaningful here.
    """
    if not leadership_signals:
        leadership_score = 0
    else:
        # company score should follow the formula using role_weight * max_indicator
        executives: List[LeadershipProfile] = []
        for s in leadership_signals:
            try:
                meta = json.loads(s.metadata_json or "{}")
                indicators = [AIBackgroundType(x) for x in meta.get("ai_indicators", [])]
                executives.append(
                    LeadershipProfile(
                        name=meta.get("executive_name", s.title or "Unknown"),
                        title=meta.get("executive_title", ""),
                        company=meta.get("company", ""),
                        ai_indicators=indicators,
                        url=s.url,
                        observed_date=meta.get("observed_date"),
                    )
                )
            except Exception:
                continue

        score_0_1 = calculate_leadership_score_0_1(executives)
        leadership_score = int(round(score_0_1 * 100))

    # placeholders for the other 3 in this aggregator
    return CompanySignalSummary(
        company_id=company_id,
        jobs_score=0,
        tech_score=0,
        patents_score=0,
        leadership_score=leadership_score,
        composite_score=0,
        last_updated_at=datetime.utcnow(),
    )


def scrape_leadership_profiles_mock(company: str = "TestCo") -> List[LeadershipProfile]:
    """
    Mock inputs (compliance-safe). Replace with manual research / provider later.
    """
    return [
        LeadershipProfile(
            name="Alex Rivera",
            title="CEO",
            company=company,
            ai_indicators=[],  # CEO no AI background
            url="https://example.com/leader1",
        ),
        LeadershipProfile(
            name="Priya Nair",
            title="CTO",
            company=company,
            ai_indicators=[AIBackgroundType.PHD_AI_ML],
            url="https://example.com/leader2",
        ),
        LeadershipProfile(
            name="Jordan Kim",
            title="CDO",
            company=company,
            ai_indicators=[AIBackgroundType.AI_COMPANY_VETERAN],
            url="https://example.com/leader3",
        ),
        LeadershipProfile(
            name="Sam Patel",
            title="VP Data",
            company=company,
            ai_indicators=[AIBackgroundType.AI_CERTIFICATION],
            url="https://example.com/leader4",
        ),
    ]



def leadership_profiles_to_aggregated_signal(
    company_id: str,
    executives: List[LeadershipProfile]
) -> ExternalSignal:
    """
    Create ONE aggregated leadership signal from all executives.
    
    Following CS2 pattern: ONE signal per category per company.
    
    Scoring:
    - Each exec has: role_weight × max(ai_indicators)
    - Company score: average across all execs
    
    Args:
        company_id: Company UUID
        executives: List of executive profiles
        
    Returns:
        Single aggregated ExternalSignal
    """
    now = datetime.utcnow()
    
    if not executives:
        # No executives - return zero score
        return ExternalSignal(
            id=sha256(f"{company_id}|leadership|no_data".encode()).hexdigest(),
            company_id=company_id,
            category=SignalCategory.leadership,
            source=SignalSource.external,
            signal_date=now,
            score=0,
            title="No leadership data available",
            url=None,
            metadata_json=json.dumps({
                "executive_count": 0,
                "company": "",
                "executives": [],
                "calculation": "No executives analyzed"
            })
        )
    
    # Calculate company-level leadership score (CS2 formula)
    score_0_1 = calculate_leadership_score_0_1(executives)
    score_0_100 = int(round(score_0_1 * 100))
    
    # Build detailed metadata with all executives
    exec_details = []
    for e in executives:
        ai_score = _max_indicator_score(e.ai_indicators)
        role_w = _role_weight(e.title)
        weighted_score = role_w * ai_score
        
        exec_details.append({
            "name": e.name,
            "title": e.title,
            "ai_indicators": [i.value for i in e.ai_indicators],
            "max_indicator_score": round(ai_score, 2),
            "role_weight": round(role_w, 2),
            "weighted_contribution": round(weighted_score, 2),
            "individual_score": int(round(ai_score * 100))
        })
    
    # Sort by role importance
    exec_details.sort(key=lambda x: x["role_weight"], reverse=True)
    
    meta = {
        "executive_count": len(executives),
        "company": executives[0].company if executives else "",
        "aggregated_score": score_0_100,
        "calculation_method": "average(role_weight × max_ai_indicator)",
        "executives": exec_details
    }
    
    # Create ONE signal for entire leadership team
    signal_id = sha256(f"{company_id}|leadership|aggregated|{now.isoformat()}".encode()).hexdigest()
    
    return ExternalSignal(
        id=signal_id,
        company_id=company_id,
        category=SignalCategory.leadership,
        source=SignalSource.external,
        signal_date=now,
        score=score_0_100,
        title=f"Leadership Team AI Expertise ({len(executives)} executives)",
        url=None,
        metadata_json=json.dumps(meta, default=str)
    )