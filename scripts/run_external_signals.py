from __future__ import annotations

import argparse

from app.pipelines.external_signals_orchestrator import run_external_signals_pipeline
from app.pipelines.tech_signals import (
    tech_inputs_to_signals,
    scrape_tech_signal_inputs_mock,
)
from app.pipelines.patent_signals import (
    patent_inputs_to_signals,
    scrape_patent_signal_inputs_mock,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run External Signals pipeline (Jobs + Tech + Patents).")
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--query", required=True, help="Job search query (e.g., 'machine learning engineer')")
    parser.add_argument("--location", default="Boston, MA")
    parser.add_argument("--sources", default="indeed,google", help="Comma-separated (indeed,google)")
    parser.add_argument("--max-per-source", type=int, default=3)

    args = parser.parse_args()
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    # --- TECH (MOCK) ---
    tech_items = scrape_tech_signal_inputs_mock(company="TestCo")
    tech_signals = tech_inputs_to_signals(company_id=args.company_id, items=tech_items)

    # --- PATENTS (MOCK) ---
    patent_items = scrape_patent_signal_inputs_mock(company="TestCo")
    patent_signals = patent_inputs_to_signals(company_id=args.company_id, items=patent_items)

    # Orchestrator still runs jobs scraping (real via JobSpy) + aggregates everything
    result = run_external_signals_pipeline(
        company_id=args.company_id,
        jobs_search_query=args.query,
        jobs_sources=sources,
        jobs_location=args.location,
        jobs_max_results_per_source=args.max_per_source,
        tech_items=tech_items,
        patent_items=patent_items,
    )

    print("\n=== External Signals Run ===")
    print("company_id:", result.company_id)
    print("jobs_signals:", len(result.jobs_signals))
    print("tech_signals:", len(result.tech_signals))
    print("patent_signals:", len(result.patent_signals))
    print("SUMMARY:", result.summary)

    # Optional: show mock signals too (debug)
    print("\n[debug] mock tech_signals:", len(tech_signals))
    print("[debug] mock patent_signals:", len(patent_signals))


if __name__ == "__main__":
    main()