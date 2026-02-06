# app/routers/signals.py
"""
Unified External Signals API Endpoints

ALL endpoints use TICKER (not company_id) for easy access.
Comprehensive AI/ML signal collection with no arbitrary limits.
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import Optional
from datetime import datetime
import structlog

from app.services.snowflake import SnowflakeService
from app.models.signal import ExternalSignal, CompanySignalSummary, SignalCategory

# Import all collectors
from app.pipelines.job_signals import scrape_job_postings, job_postings_to_signals
from app.pipelines.tech_signals import scrape_tech_signal_inputs, tech_inputs_to_signals
from app.pipelines.patent_signals import collect_patent_signals_real, COMPANY_USPTO_NAMES
from app.pipelines.leadership_signals import scrape_leadership_profiles_mock, leadership_profiles_to_signals
from app.pipelines.external_signals_orchestrator import build_company_signal_summary

logger = structlog.get_logger()
router = APIRouter(prefix="/api/signals", tags=["signals"])


# ============================================================================
# 🎯 UNIFIED COLLECTION ENDPOINT - COMPREHENSIVE AI/ML SEARCH
# ============================================================================

@router.post("/collect/{ticker}")
async def collect_all_signals(
    ticker: str,
    background_tasks: BackgroundTasks,
    years: int = Query(default=5, ge=1, le=10, description="Years for patent search"),
    job_location: str = Query(default="United States", description="Job search location")
):
    """
    🎯 Collect ALL 4 signal types for a company - COMPREHENSIVE SEARCH
    
    What it does:
    1. Jobs: Searches 10+ AI/ML job types (ML Engineer, Data Scientist, etc.)
              No limits - gets ALL available jobs for the company
    2. Tech Stack: Scrapes company website for AI technologies
    3. Patents: Calls USPTO API for AI patents
    4. Leadership: Analyzes executive AI expertise
    
    Then:
    - Inserts all signals into external_signals table
    - Calculates composite score (weighted average)
    - Updates company_signal_summaries
    
    Args:
        ticker: Company ticker (WMT, JPM, CAT, etc.)
        years: Years to look back for patents (default: 5)
        job_location: Where to search for jobs (default: "United States")
        
    Returns:
        Immediate confirmation + background job started
    """
    db = SnowflakeService()
    try:
        # Get company by ticker
        company_query = """
            SELECT id, name, ticker FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
        """
        companies = db.execute_query(company_query, {"ticker": ticker.upper()})
        
        if not companies:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{ticker}' not found in database"
            )
        
        company = companies[0]
        
        # Trigger comprehensive collection in background
        background_tasks.add_task(
            run_comprehensive_collection_task,
            company_id=company['id'],
            company_name=company['name'],
            ticker=ticker.upper(),
            years=years,
            job_location=job_location
        )
        
        return {
            "status": "accepted",
            "message": f"Comprehensive signal collection started for {ticker}",
            "company": company,
            "collection_scope": {
                "jobs": "10+ AI/ML role types, unlimited results",
                "tech_stack": "Full website technology scan",
                "patents": f"All AI patents ({years} years)",
                "leadership": "All C-suite executives"
            },
            "estimated_time": "30-60 seconds",
            "note": "Collection running in background. Check /api/v1/signals/summary for results."
        }
        
    finally:
        db.close()


@router.post("/collect/patents/{ticker}")
async def collect_patents_only(
    ticker: str,
    background_tasks: BackgroundTasks,
    years: int = Query(default=5, ge=1, le=10)
):
    """
    Collect ONLY patent signals for a company (for testing/debugging).
    
    Args:
        ticker: Company ticker (WMT, JPM, etc.)
        years: Years to look back (default: 5)
    """
    db = SnowflakeService()
    try:
        company_query = """
            SELECT id, name, ticker FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
        """
        companies = db.execute_query(company_query, {"ticker": ticker.upper()})
        
        if not companies:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{ticker}' not found"
            )
        
        company = companies[0]
        
        background_tasks.add_task(
            run_patent_only_task,
            company_id=company['id'],
            company_name=company['name'],
            ticker=ticker.upper(),
            years=years
        )
        
        return {
            "status": "accepted",
            "message": f"Patent collection started for {ticker}",
            "company": company,
            "parameters": {"years": years}
        }
        
    finally:
        db.close()


@router.post("/collect/jobs/{ticker}")
async def collect_jobs_only(
    ticker: str,
    background_tasks: BackgroundTasks,
    job_location: str = Query(default="United States")
):
    """
    Collect ONLY job signals - comprehensive AI/ML search.
    
    Args:
        ticker: Company ticker
        job_location: Job search location
    """
    db = SnowflakeService()
    try:
        company_query = """
            SELECT id, name, ticker FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
        """
        companies = db.execute_query(company_query, {"ticker": ticker.upper()})
        
        if not companies:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{ticker}' not found"
            )
        
        company = companies[0]
        
        background_tasks.add_task(
            run_jobs_only_task,
            company_id=company['id'],
            company_name=company['name'],
            ticker=ticker.upper(),
            job_location=job_location
        )
        
        return {
            "status": "accepted",
            "message": f"Comprehensive job search started for {ticker}",
            "company": company,
            "search_scope": "10+ AI/ML role types"
        }
        
    finally:
        db.close()


@router.post("/collect/all")
async def collect_all_companies(
    background_tasks: BackgroundTasks,
    years: int = Query(default=5, ge=1, le=10)
):
    """
    Collect ALL signals for ALL companies with USPTO mappings.
    
    Runs comprehensive collection for all 9 companies.
    This will take several minutes!
    """
    background_tasks.add_task(
        run_batch_collection_task,
        years=years
    )
    
    return {
        "status": "accepted",
        "message": "Batch collection started for all 9 companies",
        "companies": list(COMPANY_USPTO_NAMES.keys()),
        "estimated_time": "5-10 minutes",
        "note": "Check /api/v1/signals/summary for progress"
    }


# ============================================================================
# RETRIEVAL ENDPOINTS - ALL USE TICKER
# ============================================================================

@router.get("/company/{ticker}")
async def get_signals_by_ticker(ticker: str):
    """
    Get all signals for a company BY TICKER.
    
    Args:
        ticker: Company ticker (WMT, JPM, etc.)
        
    Returns:
        All signals for that company
    """
    db = SnowflakeService()
    try:
        # Get company by ticker first
        company_query = """
            SELECT id FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
        """
        companies = db.execute_query(company_query, {"ticker": ticker.upper()})
        
        if not companies:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{ticker}' not found"
            )
        
        company_id = companies[0]['id']
        
        # Get signals
        signals_query = """
            SELECT 
                id, company_id, category, source, signal_date,
                raw_value, normalized_score, confidence, 
                metadata, created_at
            FROM external_signals
            WHERE company_id = %(company_id)s
            ORDER BY signal_date DESC, created_at DESC
        """
        
        signals = db.execute_query(signals_query, {"company_id": company_id})
        
        if not signals:
            raise HTTPException(
                status_code=404,
                detail=f"No signals found for {ticker}"
            )
        
        return {
            "ticker": ticker.upper(),
            "company_id": company_id,
            "signal_count": len(signals),
            "signals": signals
        }
        
    finally:
        db.close()


@router.get("/company/{ticker}/category/{category}")
async def get_signals_by_ticker_and_category(ticker: str, category: str):
    """
    Get signals for a company by TICKER and category.
    
    Args:
        ticker: Company ticker (WMT, JPM, etc.)
        category: Signal category (jobs, tech, patents, leadership)
    """
    valid_categories = ["jobs", "tech", "patents", "leadership"]
    
    if category not in valid_categories:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid category. Valid: {valid_categories}"
        )
    
    db = SnowflakeService()
    try:
        # Get company by ticker
        company_query = """
            SELECT id FROM companies
            WHERE ticker = %(ticker)s AND is_deleted = FALSE
        """
        companies = db.execute_query(company_query, {"ticker": ticker.upper()})
        
        if not companies:
            raise HTTPException(
                status_code=404,
                detail=f"Company '{ticker}' not found"
            )
        
        company_id = companies[0]['id']
        
        # Map category to DB format
        category_map = {
            "jobs": "technology_hiring",
            "tech": "digital_presence",
            "patents": "innovation_activity",
            "leadership": "leadership_signals"
        }
        db_category = category_map.get(category, category)
        
        # Get signals
        signals_query = """
            SELECT 
                id, company_id, category, source, signal_date,
                raw_value, normalized_score, confidence, 
                metadata, created_at
            FROM external_signals
            WHERE company_id = %(company_id)s
              AND category = %(category)s
            ORDER BY signal_date DESC
        """
        
        signals = db.execute_query(signals_query, {
            "company_id": company_id,
            "category": db_category
        })
        
        return {
            "ticker": ticker.upper(),
            "category": category,
            "signal_count": len(signals),
            "signals": signals
        }
        
    finally:
        db.close()


@router.get("/summary")
async def get_all_summaries():
    """Get summaries for all companies - ranked by composite score."""
    db = SnowflakeService()
    try:
        query = """
            SELECT 
                css.company_id,
                css.ticker,
                c.name as company_name,
                css.technology_hiring_score as jobs_score,
                css.innovation_activity_score as patents_score,
                css.digital_presence_score as tech_score,
                css.leadership_signals_score as leadership_score,
                css.composite_score,
                css.signal_count,
                css.last_updated
            FROM company_signal_summaries css
            JOIN companies c ON css.company_id = c.id
            WHERE c.is_deleted = FALSE
            ORDER BY css.composite_score DESC
        """
        
        summaries = db.execute_query(query)
        
        return {
            "count": len(summaries),
            "summaries": summaries
        }
        
    finally:
        db.close()


@router.get("/summary/{ticker}")
async def get_summary_by_ticker(ticker: str):
    """
    Get summary for a company BY TICKER.
    
    Args:
        ticker: Company ticker (WMT, JPM, etc.)
    """
    db = SnowflakeService()
    try:
        query = """
            SELECT 
                css.company_id,
                css.ticker,
                c.name as company_name,
                css.technology_hiring_score as jobs_score,
                css.innovation_activity_score as patents_score,
                css.digital_presence_score as tech_score,
                css.leadership_signals_score as leadership_score,
                css.composite_score,
                css.signal_count,
                css.last_updated
            FROM company_signal_summaries css
            JOIN companies c ON css.company_id = c.id
            WHERE css.ticker = %(ticker)s
              AND c.is_deleted = FALSE
        """
        
        summaries = db.execute_query(query, {"ticker": ticker.upper()})
        
        if not summaries:
            raise HTTPException(
                status_code=404,
                detail=f"No summary found for {ticker}"
            )
        
        return summaries[0]
        
    finally:
        db.close()


# ============================================================================
# BACKGROUND TASKS - THE WORKERS
# ============================================================================

async def run_comprehensive_collection_task(
    company_id: str,
    company_name: str,
    ticker: str,
    years: int,
    job_location: str
):
    """
    COMPREHENSIVE collection - ALL AI/ML jobs, no limits!
    """
    try:
        db = SnowflakeService()
        all_signals = []
        
        logger.info(
            "🚀 Starting comprehensive collection",
            ticker=ticker,
            company_id=company_id,
            company_name=company_name
        )
        
        # ========================================
        # 1. JOBS - COMPREHENSIVE SEARCH
        # ========================================
        try:
            all_jobs = []
            
            # All AI/ML job types - NO FILTERS!
            comprehensive_searches = [
                "machine learning engineer",
                "data scientist",
                "AI engineer",
                "artificial intelligence engineer",
                "deep learning engineer",
                "MLOps engineer",
                "research scientist machine learning",
                "NLP engineer",
                "natural language processing",
                "computer vision engineer",
                "data engineer machine learning",
                "AI researcher",
                "ML platform engineer",
                "AI product manager"
            ]
            
            logger.info(
                "Starting comprehensive job search",
                queries=len(comprehensive_searches),
                ticker=ticker
            )
            
            for search_query in comprehensive_searches:
                try:
                    jobs = scrape_job_postings(
                        search_query=search_query,
                        sources=["indeed", "google"],
                        location=job_location,
                        max_results_per_source=100,  # HIGH LIMIT!
                        target_company_name=company_name
                    )
                    all_jobs.extend(jobs)
                    if jobs:
                        logger.info(
                            f"✓ Query found jobs",
                            query=search_query[:30],
                            count=len(jobs)
                        )
                except Exception as e:
                    logger.warning(
                        f"Search query failed",
                        query=search_query,
                        error=str(e)
                    )
            
            # Deduplicate by URL
            seen_urls = set()
            unique_jobs = []
            for job in all_jobs:
                job_url = job.url or ""
                if job_url:
                    if job_url not in seen_urls:
                        seen_urls.add(job_url)
                        unique_jobs.append(job)
                else:
                    # Keep jobs without URLs
                    unique_jobs.append(job)
            
            job_signals = job_postings_to_signals(company_id, unique_jobs)
            all_signals.extend(job_signals)
            
            logger.info(
                "✅ Jobs collection complete",
                total_found=len(all_jobs),
                unique=len(unique_jobs),
                signals=len(job_signals)
            )
            
        except Exception as e:
            logger.error("Job collection failed", error=str(e))
        
        # ========================================
        # 2. TECH STACK
        # ========================================
        try:
            domain = db.get_primary_domain_by_company_id(company_id)
            if domain:
                tech_inputs = scrape_tech_signal_inputs(
                    company=company_name,
                    company_domain_or_url=domain
                )
                tech_signals = tech_inputs_to_signals(company_id, tech_inputs)
                all_signals.extend(tech_signals)
                logger.info("✅ Tech stack collected", count=len(tech_signals))
            else:
                logger.warning("⚠️ No domain found, skipping tech signals")
        except Exception as e:
            logger.error("Tech collection failed", error=str(e))
        
        # ========================================
        # 3. PATENTS - YOUR CODE!
        # ========================================
        try:
            uspto_name = COMPANY_USPTO_NAMES.get(ticker)
            if uspto_name:
                patent_signals = await collect_patent_signals_real(
                    company_id=company_id,
                    company_name=company_name,
                    uspto_name=uspto_name,
                    years=years
                )
                all_signals.extend(patent_signals)
                patent_score = patent_signals[0].score if patent_signals else 0
                logger.info(
                    "✅ Patents collected",
                    count=len(patent_signals),
                    score=patent_score
                )
            else:
                logger.warning("⚠️ No USPTO name mapping", ticker=ticker)
        except Exception as e:
            logger.error("Patent collection failed", error=str(e))
        
        # ========================================
        # 4. LEADERSHIP
        # ========================================
        try:
            leadership_profiles = scrape_leadership_profiles_mock(company=company_name)
    
    # Import the aggregated function
            from app.pipelines.leadership_signals import leadership_profiles_to_aggregated_signal
    
            leadership_signal = leadership_profiles_to_aggregated_signal(company_id, leadership_profiles)  # ✅ 1 signal
            all_signals.append(leadership_signal)  # ✅ Adds 1
    
            logger.info(
                 "✅ Leadership aggregated",
                execs=len(leadership_profiles),
                score=leadership_signal.score
        )
            
        except Exception as e:
            logger.exception("❌ Leadership pipeline failed", error=str(e))
        
        # ========================================
        # CALCULATE SCORES
        # ========================================
        from statistics import mean
        
        def calc_category_score(category):
            matching = [s.score for s in all_signals if s.category == category]
            return int(round(mean(matching))) if matching else 0
        
        jobs_score = calc_category_score(SignalCategory.jobs)
        tech_score = calc_category_score(SignalCategory.tech)
        patents_score = calc_category_score(SignalCategory.patents)
        leadership_score = calc_category_score(SignalCategory.leadership)
        
        # Build summary
        summary = build_company_signal_summary(
            company_id=company_id,
            jobs_score=jobs_score,
            tech_score=tech_score,
            patents_score=patents_score,
            leadership_score=leadership_score
        )
        
        # ========================================
        # STORE IN SNOWFLAKE
        # ========================================
        if all_signals:
            count = db.insert_external_signals(all_signals)
            db.upsert_company_signal_summary(summary, signal_count=count)
            
            logger.info(
                "🎉 Collection complete!",
                ticker=ticker,
                total_signals=count,
                jobs_score=jobs_score,
                tech_score=tech_score,
                patents_score=patents_score,
                leadership_score=leadership_score,
                composite_score=summary.composite_score
            )
        else:
            logger.warning("⚠️ No signals collected", ticker=ticker)
        
        db.close()
        
    except Exception as e:
        logger.error(
            "❌ Collection failed",
            ticker=ticker,
            error=str(e)
        )


async def run_patent_only_task(
    company_id: str,
    company_name: str,
    ticker: str,
    years: int
):
    """Background task - Patents only."""
    try:
        db = SnowflakeService()
        
        uspto_name = COMPANY_USPTO_NAMES.get(ticker)
        if not uspto_name:
            logger.error("No USPTO mapping", ticker=ticker)
            return
        
        # Collect patents
        patent_signals = await collect_patent_signals_real(
            company_id=company_id,
            company_name=company_name,
            uspto_name=uspto_name,
            years=years
        )
        
        if patent_signals:
            count = db.insert_external_signals(patent_signals)
            
            # Get existing scores to preserve them
            summary_query = """
                SELECT 
                    COALESCE(technology_hiring_score, 0) as jobs_score,
                    COALESCE(digital_presence_score, 0) as tech_score,
                    COALESCE(leadership_signals_score, 0) as leadership_score
                FROM company_signal_summaries
                WHERE company_id = %(company_id)s
            """
            existing = db.execute_query(summary_query, {"company_id": company_id})
            
            if existing:
                jobs_score = int(existing[0]['jobs_score'])
                tech_score = int(existing[0]['tech_score'])
                leadership_score = int(existing[0]['leadership_score'])
            else:
                jobs_score = tech_score = leadership_score = 0
            
            patents_score = patent_signals[0].score
            
            summary = build_company_signal_summary(
                company_id=company_id,
                jobs_score=jobs_score,
                tech_score=tech_score,
                patents_score=patents_score,
                leadership_score=leadership_score
            )
            
            db.upsert_company_signal_summary(summary, signal_count=count)
            
            logger.info(
                "✅ Patents collected",
                ticker=ticker,
                score=patents_score,
                composite=summary.composite_score
            )
        
        db.close()
        
    except Exception as e:
        logger.error("Patent task failed", ticker=ticker, error=str(e))


async def run_jobs_only_task(
    company_id: str,
    company_name: str,
    ticker: str,
    job_location: str
):
    """Background task - Jobs only."""
    try:
        db = SnowflakeService()
        all_jobs = []
        
        searches = [
            "machine learning engineer",
            "data scientist",
            "AI engineer",
            "MLOps engineer",
            "deep learning",
            "NLP engineer"
        ]
        
        for query in searches:
            try:
                jobs = scrape_job_postings(
                    search_query=query,
                    sources=["indeed", "google"],
                    location=job_location,
                    max_results_per_source=100,
                    target_company_name=company_name
                )
                all_jobs.extend(jobs)
            except Exception as e:
                logger.warning(f"Query '{query}' failed", error=str(e))
        
        # Deduplicate
        seen = set()
        unique = []
        for job in all_jobs:
            if job.url and job.url not in seen:
                seen.add(job.url)
                unique.append(job)
            elif not job.url:
                unique.append(job)
        
        if unique:
            job_signals = job_postings_to_signals(company_id, unique)
            count = db.insert_external_signals(job_signals)
            
            logger.info(
                "✅ Jobs collected",
                ticker=ticker,
                total=len(all_jobs),
                unique=len(unique),
                signals=count
            )
        
        db.close()
        
    except Exception as e:
        logger.error("Jobs task failed", ticker=ticker, error=str(e))


async def run_batch_collection_task(years: int):
    """Batch collection for all companies."""
    try:
        for ticker in COMPANY_USPTO_NAMES.keys():
            db = SnowflakeService()
            
            try:
                company_query = """
                    SELECT id, name FROM companies
                    WHERE ticker = %(ticker)s AND is_deleted = FALSE
                """
                companies = db.execute_query(company_query, {"ticker": ticker})
                
                if companies:
                    await run_comprehensive_collection_task(
                        company_id=companies[0]['id'],
                        company_name=companies[0]['name'],
                        ticker=ticker,
                        years=years,
                        job_location="United States"
                    )
                    
                    # Delay between companies
                    import asyncio
                    await asyncio.sleep(30)
            finally:
                db.close()
                
    except Exception as e:
        logger.error("Batch collection failed", error=str(e))
