from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from app.services.snowflake import SnowflakeService
from app.pipelines.external_signals_orchestrator import run_external_signals_pipeline

# Digital Presence (REAL)
from app.pipelines.tech_signals import tech_inputs_to_signals, scrape_tech_signal_inputs

# Patents + Leadership (still mock for now in your repo)
from app.pipelines.patent_signals import patent_inputs_to_signals, scrape_patent_signal_inputs_mock
from app.pipelines.leadership_signals import scrape_leadership_profiles_mock


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run External Signals pipeline (Jobs(company-specific) + Digital Presence(real) + Patents(mock) + Leadership(mock))."
    )
    parser.add_argument("--company-id", required=True, help="Must be an existing companies.id in Snowflake")
    parser.add_argument("--query", required=True, help="Job search query (e.g., 'machine learning engineer')")
    parser.add_argument("--location", default="Boston, MA")
    parser.add_argument("--sources", default="indeed,google", help="Comma-separated (indeed,google)")
    parser.add_argument("--max-per-source", type=int, default=3)

    args = parser.parse_args()
    sources: List[str] = [s.strip() for s in args.sources.split(",") if s.strip()]

    # ✅ IMPORTANT: Create ONE SnowflakeService and reuse it
    svc = SnowflakeService()

    # -------------------
    # A) Fetch company name + domain from Snowflake (real source)
    # -------------------
    company = svc.get_company(args.company_id)
    if not company:
        print(f"❌ Company not found for company_id={args.company_id}. Check companies table.", file=sys.stderr)
        sys.exit(1)

    company_name: str = company.get("name") or ""
    if not company_name:
        print(f"❌ Company name is missing for company_id={args.company_id}.", file=sys.stderr)
        sys.exit(1)

    domain_url: Optional[str] = svc.get_primary_domain_by_company_id(args.company_id)
    if not domain_url:
        print(
            f"❌ No primary domain found in company_domains for company_id={args.company_id}. "
            f"Insert a row into company_domains first.",
            file=sys.stderr,
        )
        sys.exit(1)

    # -------------------
    # B) Digital Presence (REAL) using company_name + domain_url
    # -------------------
    tech_items = scrape_tech_signal_inputs(
        company=company_name,
        company_domain_or_url=domain_url,
    )
    tech_signals = tech_inputs_to_signals(company_id=args.company_id, items=tech_items)

    # -------------------
    # C) Patents (MOCK for now)
    # -------------------
    patent_items = scrape_patent_signal_inputs_mock(company=company_name)
    patent_signals = patent_inputs_to_signals(company_id=args.company_id, items=patent_items)

    # -------------------
    # D) Leadership (MOCK for now)
    # -------------------
    leadership_profiles = scrape_leadership_profiles_mock(company=company_name)

    # -------------------
    # E) Orchestrator runs jobs scraping (real via JobSpy) + aggregates everything
    # ✅ NEW: pass company name/domain so Technology Hiring becomes company-specific
    # -------------------
    result = run_external_signals_pipeline(
        company_id=args.company_id,
        jobs_search_query=args.query,
        jobs_sources=sources,
        jobs_location=args.location,
        jobs_max_results_per_source=args.max_per_source,
        jobs_target_company_name=company_name,
        jobs_target_company_domain_url=domain_url,
        tech_items=tech_items,
        patent_items=patent_items,
        leadership_profiles=leadership_profiles,
    )

    # -------------------
    # F) Write to Snowflake (same svc)
    # -------------------
    all_signals = result.jobs_signals + result.tech_signals + result.patent_signals + result.leadership_signals
    n = svc.insert_external_signals(all_signals)
    svc.upsert_company_signal_summary(result.summary, signal_count=n)

    print(f"\n✅ Inserted {n} external_signals rows into Snowflake")

    print("\n=== External Signals Run ===")
    print("company_id:", result.company_id)
    print("company_name:", company_name)
    print("domain_url:", domain_url)
    print("jobs_signals:", len(result.jobs_signals))
    print("digital_presence_signals:", len(result.tech_signals))
    print("patent_signals:", len(result.patent_signals))
    print("leadership_signals:", len(result.leadership_signals))
    print("SUMMARY:", result.summary)

    # Debug
    print("\n[debug] digital_presence_items(real):", len(tech_items))
    print("[debug] digital_presence_signals(real):", len(tech_signals))
    print("[debug] patent_signals(mock):", len(patent_signals))
    print("[debug] leadership_profiles(mock):", len(leadership_profiles))


if __name__ == "__main__":
    main()