# Case Study 2: Evidence Collection

**"What Companies Say vs. What They Do"**

**Course:** Big Data and Intelligent Analytics  
**Instructor:** Sri Krishnamurthy — QuantUniversity  
**Term:** Spring 2026

**Team 3:**
- Vaishnavi Srinivas
- Ishaan Samel
- Ayush Fulsundar

---

## 🧠 Project Overview

This project implements the **Evidence Collection layer** of the PE-OrgAIR platform. Building on **Case Study 1 (Platform Foundation)**, this case study focuses on ingesting, processing, and persisting **verifiable evidence** that reflects a company's **actual AI investment**, not just public claims.

### Evidence Types

We collect and store two types of evidence:

1. **What companies say** → SEC filings (10-K, 10-Q, 8-K)
2. **What companies do** → External signals (jobs, tech stack, patents, leadership)

All evidence is normalized, scored, and persisted in **Snowflake**, forming the foundation for AI-readiness scoring in future case studies.

---

## ⚖️ System Architecture

### High-level Flow
```
External Sources
├── SEC EDGAR (10-K, 10-Q, 8-K)
├── Job Boards (Indeed, Google Jobs)
├── Technology Stack (BuiltWith / SimilarTech)
├── Patents (USPTO - mock)
└── Leadership Profiles (manual / CSV / mock)
    ↓
Evidence Collection Pipelines
    ↓
Snowflake (Documents, Chunks, Signals, Summaries)
```

### Key Design Principle

**SEC filings capture *intent*, while external signals capture *execution*.**

---

## 📂 Project Structure
```
PE_OrgAIR_CaseStudy2/
├── app/
│   ├── core/
│   │   └── deps.py                     # Dependency injection
│   │
│   ├── database/
│   │   ├── schema.sql                  # Core schema
│   │   └── schema_case_study_2.sql     # CS2-specific tables
│   │
│   ├── models/
│   │   ├── assessment.py               # Assessment data models
│   │   ├── company.py                  # Company entities
│   │   ├── dimension.py                # Scoring dimensions
│   │   ├── document.py                 # SEC filing models
│   │   ├── evidence.py                 # Evidence structures
│   │   ├── industry.py                 # Industry classifications
│   │   └── signal.py                   # External signals
│   │
│   ├── pipelines/
│   │   ├── sec_edgar.py                # SEC EDGAR data ingestion
│   │   ├── document_parser_from_s3.py  # Parse docs from S3
│   │   ├── document_text_cleaner.py    # Text preprocessing
│   │   ├── document_chunker_s3.py      # Semantic chunking
│   │   ├── job_signals.py              # Job posting scraper
│   │   ├── tech_signals.py             # Tech stack detection
│   │   ├── patent_signals.py           # Patent analysis
│   │   ├── leadership_signals.py       # Leadership scoring
│   │   └── external_signals_orchestrator.py  # Signal coordinator
│   │
│   ├── routers/
│   │   ├── companies.py                # Company endpoints
│   │   ├── assessments.py              # Assessment APIs
│   │   ├── dimension.py                # Dimension management
│   │   ├── documents.py                # Document retrieval
│   │   ├── signals.py                  # Signal endpoints
│   │   └── health.py                   # Health checks
│   │
│   ├── services/
│   │   ├── snowflake.py                # Snowflake connector
│   │   ├── s3_storage.py               # S3 operations
│   │   └── redis_cache.py              # Redis caching layer
│   │
│   ├── streamlit_app/
│   │   └── app.py                      # Dashboard UI
│   │
│   ├── config.py                       # Configuration management
│   └── main.py                         # FastAPI entrypoint
│
├── scripts/
│   ├── run_sec_edgar.py                # Execute SEC pipeline
│   ├── run_external_signals.py         # Run signal collection
│   ├── parse_document.py               # Parse individual docs
│   ├── clean_documents_from_s3.py      # Clean S3 documents
│   ├── chunk_documents_from_s3.py      # Chunk S3 documents
│   ├── backfill_companies.py           # Populate company data
│   └── company_uspto_names.py          # USPTO name mapping
│
├── data/
│   ├── raw/                            # Raw downloaded data
│   ├── processed/                      # Processed outputs
│   └── samples/                        # Sample datasets
│
├── docs/
│   └── evidence_report.md              # Analysis & findings
│
├── tests/                              # Unit & integration tests
├── Dockerfile                          # Container definition
├── docker-compose.yml                  # Multi-service orchestration
├── requirements.txt                    # Python dependencies
├── pyproject.toml                      # Poetry configuration
└── README.md                           # Project documentation
```

### Key Components

#### 🔧 **Core Application** (`app/`)
- **Models**: Pydantic schemas for data validation
- **Pipelines**: ETL workflows for evidence collection
- **Routers**: RESTful API endpoints
- **Services**: External system integrations (Snowflake, S3, Redis)

#### 📜 **Scripts** (`scripts/`)
Standalone executables for:
- Data ingestion and processing
- Pipeline orchestration
- Database backfilling

#### 🗄️ **Data** (`data/`)
- **raw/**: Unprocessed source files
- **processed/**: Cleaned and transformed data
- **samples/**: Test datasets

#### 🐳 **Infrastructure**
- **Docker**: Containerized deployment
- **docker-compose**: Local development stack

## 📊 Evidence Pipelines Implemented

### 1️⃣ SEC EDGAR Pipeline (Lab 3)

- Downloads **10-K, 10-Q, 8-K** filings for 10 target companies
- Supports **PDF and HTML** formats
- Extracts AI-relevant sections:
  - Item 1 – Business
  - Item 1A – Risk Factors
  - Item 7 – MD&A
- Implements **semantic chunking with overlap**
- Deduplicates documents using **SHA-256 content hashing**
- Tracks document lifecycle via a **document registry**

**Stored in:**
- `documents`
- `document_chunks`

---

### 2️⃣ External Signals Pipeline (Lab 4)

#### 🔹 Technology Hiring Signals

- Scrapes job postings from **Indeed & Google Jobs**
- Filters AI-related roles using keyword and skill heuristics
- Normalizes hiring intensity to a **0–100 score**
- Handles company aliases (e.g., JPMorgan, Chase, JPMC)

#### 🔹 Digital Presence Signals

- Detects AI-related technologies (ML frameworks, cloud ML, AI APIs)
- Scores based on:
  - Number of AI technologies
  - Coverage across AI categories

#### 🔹 Innovation / Patent Signals

- Mock USPTO ingestion
- Scores AI patent volume, recency, and category diversity

#### 🔹 Leadership Signals

- Executive-level AI commitment scoring
- Uses role-weighted and indicator-based scoring
- One signal per executive, aggregated at company level

**Stored in:**
- `external_signals`
- `company_signal_summaries`

---

## 🗄️ Data Persistence (Snowflake)

### Core Tables

- `documents`
- `document_chunks`
- `external_signals`
- `company_signal_summaries`

### Key Guarantees

- All signals stored with rich metadata (JSON VARIANT)
- Scores normalized to **0–100**
- Composite score computed using weighted aggregation
- Signals traceable to source and timestamp

---

## 📈 Scoring Model

| Signal Category | Weight |
|----------------|--------|
| Technology Hiring | 0.30 |
| Innovation Activity | 0.25 |
| Digital Presence | 0.25 |
| Leadership Signals | 0.20 |

**Composite Score = weighted sum of all four categories.**

---

## ▶️ How to Run

### Run External Signals for a Company
```bash
poetry run python scripts/run_external_signals.py \
  --company-id <UUID> \
  --query "machine learning engineer" \
  --location "United States" \
  --sources indeed,google \
  --max-per-source 25
```

### Verify Data in Snowflake
```sql
SELECT * FROM external_signals;
SELECT * FROM company_signal_summaries;
```

---

## 📄 Evidence Report

View the complete analysis and findings:

[Evidence Collection Report](https://docs.google.com/document/d/1uM8F2Y0ZmF4nhfrEKaGMd3pm_phT4vguyAot3XAxEt4/edit?tab=t.0)

The report includes:
- Company-wise document counts
- Signal scores by category
- Composite scores
- Observed "say vs do" gaps
- Data quality notes

## 🎯 Next Steps

This evidence layer feeds into **Case Study 3: AI-Readiness Scoring**, where we'll build machine learning models to predict company AI maturity based on the collected evidence.

---

## 📦 Requirements

See `requirements.txt` for full dependencies. Key packages:
- `snowflake-connector-python`
- `requests`
- `beautifulsoup4`
- `python-dotenv`
- `pandas`

---

## 👥 Team Contributions

- **Vaishnavi Srinivas** – External signals orchestration
- **Ishaan Samel** – Snowflake integration, data quality validation
- **Ayush Fulsundar** –  SEC EDGAR ingestion, document parsing, cleaning, and chunking

---
## 🎥 Demo Video

Watch our project demonstration:

[![Demo Video](https://img.shields.io/badge/Watch-Demo%20Video-red?style=for-the-badge&logo=google-drive)](https://drive.google.com/drive/folders/1bNFGsU0ojkWythDrCrzsGkeT6hBsrS48)

[📹 View Demo Video on Google Drive](https://drive.google.com/drive/folders/1bNFGsU0ojkWythDrCrzsGkeT6hBsrS48)

### 📚 Interactive Codelab

Follow our step-by-step interactive tutorial:

**[📖 Open Codelab: Evidence Collection - What Companies Say vs. What They Do](https://codelabs-preview.appspot.com/?file_id=1QpfDSNgSKchIRUqo1WTqa71V0DYc7TCMqaicj1PJAoU#1)**

## 📝 License

Academic project for QuantUniversity — Spring 2026
