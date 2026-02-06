from __future__ import annotations

import hashlib
import os
import time
from datetime import date
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, SSLError
from dotenv import load_dotenv
from sec_edgar_downloader import Downloader

from app.services.snowflake import SnowflakeService

load_dotenv()

# Default targets (for standalone script usage)
DEFAULT_TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
DEFAULT_FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]

SEC_REQUEST_SLEEP_SECONDS = float(os.getenv("SEC_SLEEP_SECONDS", "0.75"))


def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def latest_download_folder(ticker: str, filing_type: str) -> Path | None:
    base = Path("data/raw") / "sec-edgar-filings" / ticker / filing_type
    if not base.exists():
        return None
    subdirs = [p for p in base.iterdir() if p.is_dir()]
    if not subdirs:
        return None
    subdirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return subdirs[0]


def pick_main_file(folder: Path) -> Path | None:
    candidates = list(folder.rglob("full-submission.txt"))
    if not candidates:
        candidates = list(folder.rglob("*.txt")) + list(folder.rglob("*.html")) + list(folder.rglob("*.htm"))
    if not candidates:
        candidates = [p for p in folder.rglob("*") if p.is_file()]
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_size, reverse=True)
    return candidates[0]


def filing_type_for_paths(filing_type: str) -> str:
    # stable, URL/S3 safe
    t = filing_type.upper().strip()
    t = t.replace(" ", "")        # DEF 14A -> DEF14A
    t = t.replace("-", "")        # DEF-14A -> DEF14A
    return t


def build_sec_source_url(download_folder: Path, main_file: Path) -> str | None:
    """
    Best-effort: sec_edgar_downloader creates a folder like:
      0000320193-25-000079
    We can build:
      https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/{filename}
    """
    accession = download_folder.name  # e.g., 0000320193-25-000079
    parts = accession.split("-")
    if len(parts) < 3:
        return None

    cik = parts[0].lstrip("0") or "0"
    accession_nodashes = accession.replace("-", "")
    filename = main_file.name

    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/{filename}"


def _run_collection(
    tickers: list[str],
    filing_types: list[str],
    limit_per_type: int = 1
) -> dict[str, int]:
    """
    Core collection logic - reusable by both main() and collect_for_tickers().
    
    Args:
        tickers: List of company tickers to collect
        filing_types: List of filing types (10-K, 10-Q, etc.)
        limit_per_type: Number of filings to download per type
        
    Returns:
        Dictionary with statistics
    """
    email = require_env("SEC_EDGAR_USER_AGENT_EMAIL")
    bucket = require_env("S3_BUCKET_NAME")
    region = os.getenv("AWS_REGION", "us-east-1")

    download_root = Path("data/raw")
    download_root.mkdir(parents=True, exist_ok=True)

    dl = Downloader("OrgAIR", email, str(download_root))

    s3 = boto3.client(
        "s3",
        region_name=region,
        config=Config(
            retries={"max_attempts": 12, "mode": "adaptive"},
            connect_timeout=30,
            read_timeout=120,
        ),
    )

    sf = SnowflakeService()

    # Build dynamic IN list for SQL query
    placeholders = ",".join([f"%(t{i})s" for i in range(len(tickers))])

    company_rows = sf.execute_query(
        f"""
        SELECT id, ticker
        FROM companies
        WHERE is_deleted = FALSE
          AND UPPER(ticker) IN ({placeholders})
        """,
        {f"t{i}": tickers[i] for i in range(len(tickers))},
    )

    ticker_to_company: dict[str, str] = {}
    for r in company_rows:
        tid = r.get("TICKER") if "TICKER" in r else r.get("ticker")
        cid = r.get("ID") if "ID" in r else r.get("id")
        ticker_to_company[str(tid).upper()] = str(cid)

    missing = [t for t in tickers if t not in ticker_to_company]
    if missing:
        raise RuntimeError(f"Missing in companies table: {missing} (insert targets first)")

    # Statistics
    stats = {
        "inserted": 0,
        "skipped_dedup": 0,
        "skipped_missing_file": 0,
        "skipped_sec_download_error": 0,
        "skipped_s3_upload_error": 0
    }
    
    run_date = date.today().isoformat()

    for ticker in tickers:
        for filing_type in filing_types:
            # SEC download
            try:
                dl.get(filing_type, ticker, limit=limit_per_type)  # ✅ Use dynamic limit!
            except Exception as e:
                print(f"⚠️ SEC download failed {ticker} {filing_type}: {e}")
                stats["skipped_sec_download_error"] += 1
                time.sleep(SEC_REQUEST_SLEEP_SECONDS)
                continue

            time.sleep(SEC_REQUEST_SLEEP_SECONDS)

            folder = latest_download_folder(ticker, filing_type)
            if not folder:
                print(f"⚠️ No download folder {ticker} {filing_type}")
                stats["skipped_missing_file"] += 1
                continue

            main_file = pick_main_file(folder)
            if not main_file:
                print(f"⚠️ No main file {ticker} {filing_type}")
                stats["skipped_missing_file"] += 1
                continue

            content_hash = sha256_file(main_file)

            # Deduplication check
            existing = sf.execute_query(
                """
                SELECT id
                FROM documents
                WHERE ticker = %(ticker)s
                  AND filing_type = %(filing_type)s
                  AND content_hash = %(content_hash)s
                LIMIT 1
                """,
                {"ticker": ticker, "filing_type": filing_type, "content_hash": content_hash},
            )
            if existing:
                stats["skipped_dedup"] += 1
                continue

            doc_id = str(uuid4())
            ext = main_file.suffix or ".txt"
            ft_path = filing_type_for_paths(filing_type)

            s3_key = f"sec/{ticker}/{ft_path}/{run_date}/{doc_id}{ext}"

            # S3 upload
            try:
                s3.upload_file(str(main_file), bucket, s3_key)
            except (SSLError, BotoCoreError, ClientError) as e:
                print(f"⚠️ S3 upload failed {ticker} {filing_type}: {e}")
                stats["skipped_s3_upload_error"] += 1
                continue

            source_url = build_sec_source_url(folder, main_file)

            # Insert documents row
            sf.execute_update(
                """
                INSERT INTO documents
                  (id, company_id, ticker, filing_type, filing_date, source_url, local_path, s3_key,
                   content_hash, status, created_at)
                VALUES
                  (%(id)s, %(company_id)s, %(ticker)s, %(filing_type)s, CURRENT_DATE(),
                   %(source_url)s, %(local_path)s, %(s3_key)s, %(content_hash)s, 'downloaded', CURRENT_TIMESTAMP())
                """,
                {
                    "id": doc_id,
                    "company_id": ticker_to_company[ticker],
                    "ticker": ticker,
                    "filing_type": filing_type,
                    "source_url": source_url,
                    "local_path": str(main_file),
                    "s3_key": s3_key,
                    "content_hash": content_hash,
                },
            )

            stats["inserted"] += 1
            print(f"✅ {ticker} {filing_type}: documents.id={doc_id}")

    # Print summary
    print("\n=== SUMMARY ===")
    print(f"Inserted documents: {stats['inserted']}")
    print(f"Skipped dedup: {stats['skipped_dedup']}")
    print(f"Skipped missing file: {stats['skipped_missing_file']}")
    print(f"Skipped SEC download errors: {stats['skipped_sec_download_error']}")
    print(f"Skipped S3 upload errors: {stats['skipped_s3_upload_error']}")
    
    return stats


# ============================================================================
# API-FRIENDLY FUNCTION (for FastAPI router to call)
# ============================================================================

def collect_for_tickers(
    tickers: list[str],
    filing_types: list[str],
    limit_per_type: int = 1
) -> dict[str, int]:
    """
    API-friendly entrypoint for document collection.
    
    Downloads SEC filings for specified tickers, uploads to S3,
    and inserts metadata into documents table.
    
    Args:
        tickers: List of company tickers (e.g., ['WMT', 'JPM'])
        filing_types: List of filing types (e.g., ['10-K', '10-Q'])
        limit_per_type: Number of filings to download per type (default: 1)
        
    Returns:
        Dictionary with collection statistics:
        {
            "inserted": 5,
            "skipped_dedup": 2,
            "skipped_missing_file": 0,
            ...
        }
        
    Example:
        stats = collect_for_tickers(
            tickers=['WMT', 'JPM'],
            filing_types=['10-K', '10-Q'],
            limit_per_type=2
        )
    """
    # Normalize tickers to uppercase
    tickers = [t.upper().strip() for t in tickers]
    
    print(f"\n{'='*60}")
    print(f"SEC EDGAR Collection Started")
    print(f"{'='*60}")
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Filing Types: {', '.join(filing_types)}")
    print(f"Limit per type: {limit_per_type}")
    print(f"{'='*60}\n")
    
    # Call core collection logic
    stats = _run_collection(tickers, filing_types, limit_per_type)
    
    return stats


# ============================================================================
# STANDALONE SCRIPT MODE (for running from command line)
# ============================================================================

def main() -> None:
    """
    Main function for standalone script execution.
    Uses default TARGET_TICKERS and FILING_TYPES.
    """
    _run_collection(
        tickers=DEFAULT_TARGET_TICKERS,
        filing_types=DEFAULT_FILING_TYPES,
        limit_per_type=1
    )


if __name__ == "__main__":
    main()