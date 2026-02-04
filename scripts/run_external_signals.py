from __future__ import annotations

import argparse

from app.pipelines.external_signals_orchestrator import run_external_signals_pipeline
from app.pipelines.tech_signals import TechSignalInput
from app.pipelines.patent_signals import PatentSignalInput


def main() -> None:
    parser = argparse.ArgumentParser(description="Run External Signals pipeline (Jobs + Tech + Patents).")
    parser.add_argument("--company-id", required=True)
    parser.add_argument("--query", required=True, help="Job search query (e.g., 'machine learning engineer')")
    parser.add_argument("--location", default="Boston, MA")
    parser.add_argument("--sources", default="indeed,google", help="Comma-separated (indeed,google)")
    parser.add_argument("--max-per-source", type=int, default=3)

    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]

    # Demo inputs for tech/patents (replace later with real collected items)
    tech_items = [
        TechSignalInput(
            title="AI Platform Launch",
            description="LLM agent workflow with RAG using Azure and Kubernetes",
            company="DemoCo",
            url="https://example.com/tech1",
        ),
        TechSignalInput(
            title="Open source release",
            description="Open source GitHub repo for evaluation pipelines",
            company="DemoCo",
            url="https://example.com/tech2",
        ),
    ]

    patent_items = [
        PatentSignalInput(
            title="Neural network model for generative text",
            abstract="A large language model uses embeddings and transformer layers for NLP tasks",
            company="DemoCo",
            url="https://example.com/patent1",
            published_date="2025-11-10",
        )
    ]

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


if __name__ == "__main__":
    main()