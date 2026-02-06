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

TARGET_TICKERS = ["CAT", "DE", "UNH", "HCA", "ADP", "PAYX", "WMT", "TGT", "JPM", "GS"]
FILING_TYPES = ["10-K", "10-Q", "8-K", "DEF 14A"]

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

    cik = parts[0].lstrip("0") or "0"  # SEC URL uses no leading zeros sometimes; but both often work
    accession_nodashes = accession.replace("-", "")
    filename = main_file.name

    return f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodashes}/{filename}"


def main() -> None:
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

    # --- dynamic IN list ---
    placeholders = ",".join([f"%(t{i})s" for i in range(len(TARGET_TICKERS))])

    company_rows = sf.execute_query(
        f"""
        SELECT id, ticker
        FROM companies
        WHERE is_deleted = FALSE
          AND UPPER(ticker) IN ({placeholders})
        """,
        {f"t{i}": TARGET_TICKERS[i] for i in range(len(TARGET_TICKERS))},
    )

    ticker_to_company: dict[str, str] = {}
    for r in company_rows:
        tid = r.get("TICKER") if "TICKER" in r else r.get("ticker")
        cid = r.get("ID") if "ID" in r else r.get("id")
        ticker_to_company[str(tid).upper()] = str(cid)

    missing = [t for t in TARGET_TICKERS if t not in ticker_to_company]
    if missing:
        raise RuntimeError(f"Missing in companies table: {missing} (insert targets first)")

    inserted = skipped_dedup = skipped_missing_file = skipped_sec_download_error = skipped_s3_upload_error = 0
    run_date = date.today().isoformat()

    for ticker in TARGET_TICKERS:
        for filing_type in FILING_TYPES:
            # --- SEC download ---
            try:
                dl.get(filing_type, ticker, limit=1)
            except Exception as e:
                print(f"⚠️ SEC download failed {ticker} {filing_type}: {e}")
                skipped_sec_download_error += 1
                time.sleep(SEC_REQUEST_SLEEP_SECONDS)
                continue

            time.sleep(SEC_REQUEST_SLEEP_SECONDS)

            folder = latest_download_folder(ticker, filing_type)
            if not folder:
                print(f"⚠️ No download folder {ticker} {filing_type}")
                skipped_missing_file += 1
                continue

            main_file = pick_main_file(folder)
            if not main_file:
                print(f"⚠️ No main file {ticker} {filing_type}")
                skipped_missing_file += 1
                continue

            content_hash = sha256_file(main_file)

            # --- dedup ---
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
                skipped_dedup += 1
                continue

            doc_id = str(uuid4())
            ext = main_file.suffix or ".txt"
            ft_path = filing_type_for_paths(filing_type)

            s3_key = f"sec/{ticker}/{ft_path}/{run_date}/{doc_id}{ext}"

            # --- S3 upload ---
            try:
                s3.upload_file(str(main_file), bucket, s3_key)
            except (SSLError, BotoCoreError, ClientError) as e:
                print(f"⚠️ S3 upload failed {ticker} {filing_type}: {e}")
                skipped_s3_upload_error += 1
                continue

            source_url = build_sec_source_url(folder, main_file)

            # --- Insert documents row ---
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

            inserted += 1
            print(f"✅ {ticker} {filing_type}: documents.id={doc_id}")

    print("\n=== SUMMARY ===")
    print("Inserted documents:", inserted)
    print("Skipped dedup:", skipped_dedup)
    print("Skipped missing file:", skipped_missing_file)
    print("Skipped SEC download errors:", skipped_sec_download_error)
    print("Skipped S3 upload errors:", skipped_s3_upload_error)


if __name__ == "__main__":
    main()
