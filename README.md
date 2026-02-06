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
│   ├── pipelines/
│   │   ├── sec_edgar.py
│   │   ├── document_parser.py
│   │   ├── job_signals.py
│   │   ├── tech_signals.py
│   │   ├── patent_signals.py
│   │   ├── leadership_signals.py
│   │   └── external_signals_orchestrator.py
│   ├── models/
│   │   ├── document.py
│   │   ├── signal.py
│   │   └── evidence.py
│   └── services/
│       └── snowflake.py
├── scripts/
│   └── run_external_signals.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── samples/
├── docs/
│   └── evidence_report.md
├── README.md
└── requirements.txt
```

---

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

The detailed **Evidence Collection Report** is available here:

- `docs/evidence_report.md`

**Includes:**
- Company-wise document counts
- Signal scores by category
- Composite scores
- Observed "say vs do" gaps
- Data quality notes

---

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
- **Ayush Fulsundar** – scoring modelSEC EDGAR pipeline, document processing

---

## 📝 License

Academic project for QuantUniversity — Spring 2026
