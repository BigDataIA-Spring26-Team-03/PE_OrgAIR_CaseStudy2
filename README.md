PE Org-AI-R – Case Study 1: Platform Foundation
📌 Overview

This repository implements the platform foundation for the PE Org-AI-R (Private Equity Organizational AI Readiness) system.

The goal of Case Study 1 is to build a production-grade API foundation that supports:

Company onboarding

AI readiness assessments

Dimension-level scoring

Strong validation, testing, and extensibility

The system is designed to be configuration-driven and cloud-ready, following patterns introduced in Week 1 & Week 2 labs.

🧱 Architecture Summary

Core principles:

Clean separation of concerns

Strong input validation at the API boundary

Replaceable persistence layer (in-memory → Snowflake)

Test-driven confidence in business rules

Architecture Layers

FastAPI – API layer and routing

Pydantic – Domain models and validation

In-Memory Store – Temporary persistence for Case Study 1
(Designed to be replaced by Snowflake in later case studies)

Tests (pytest) – Validation of business logic and API behavior

⚠️ Snowflake, Redis, and S3 are part of the target architecture but are intentionally not fully wired in Case Study 1.
This aligns with the course’s phased approach (Lab 1 → Lab 2 → later case studies).

📁 Project Structure
app/
├── models/               # Pydantic domain models
│   ├── company.py
│   ├── assessment.py
│   └── dimension.py
│
├── routers/              # API endpoints
│   ├── health.py
│   ├── companies.py
│   ├── assessments.py
│   └── dimension_scores.py
│
├── services/             # External integrations (future: Snowflake, Redis)
├── config.py             # Application settings
└── main.py               # FastAPI application entry point

tests/
├── test_models.py        # Model-level validation tests
└── test_api.py           # API endpoint tests

docker/                   # Docker assets (used in later phases)
.env.example              # Environment variable template
requirements.txt
README.md

🚀 API Endpoints (Case Study 1 Scope)
Health

GET /health
Returns service health and dependency placeholders.

Companies

POST /api/v1/companies

GET /api/v1/companies

GET /api/v1/companies/{id}

PUT /api/v1/companies/{id}

DELETE /api/v1/companies/{id} (soft delete)

Assessments

POST /api/v1/assessments

GET /api/v1/assessments

GET /api/v1/assessments/{id}

PUT /api/v1/assessments/{id}/status

Dimension Scores

POST /api/v1/assessments/{id}/scores

GET /api/v1/assessments/{id}/scores

DELETE /api/v1/assessments/{id}/scores/{dimension}

All endpoints are documented automatically via Swagger:

http://localhost:8000/docs

🛠️ Tech Stack

FastAPI – API framework

Pydantic (v2) – Data validation

pytest – Testing

Snowflake – Target analytical database (Lab 2+)

Redis – Target caching layer (Lab 2+)

AWS S3 – Target object storage

Docker – Containerization (later phase)

⚙️ Local Setup (Lab 1 Aligned)
1️⃣ Create virtual environment
python -m venv .venv


Activate:

Windows

.venv\Scripts\activate


Mac/Linux

source .venv/bin/activate

2️⃣ Install dependencies
pip install -r requirements.txt

3️⃣ Environment variables
cp .env.example .env


Fill values as needed.
⚠️ No secrets are committed to Git.

4️⃣ Run the application
uvicorn app.main:app --reload

🧪 Testing

Run all tests:

pytest -q


Tests validate:

Input constraints (bounds, enums)

Cross-field logic (confidence intervals)

API behavior (status codes, soft deletes)

Dimension score handling

🧠 Design Decisions (Why This Matters)

In-memory storage is used deliberately as a scaffolding layer
→ enables fast iteration and clean API contracts
→ swapped with Snowflake without changing endpoints

Enums + validation enforce correctness at the boundary

Soft deletes preserve auditability

Hierarchical routing mirrors domain relationships

These decisions directly reflect Week 1 platform setup and Week 2 configuration-driven architecture principles.

🔮 Next Phases (Out of Scope for Case Study 1)

Snowflake persistence layer

Redis-based configuration caching

Scoring aggregation logic

CI/CD and container orchestration

👥 Team Workflow

Feature development on individual branches

Pull requests into main

Tests required before merge

✅ Case Study 1 Status

✔ Platform foundation complete
✔ All required APIs implemented
✔ Validation & tests in place
✔ Ready for persistence & scaling layers