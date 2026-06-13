# Medical Claims ETL Pipeline — Hospital Viana

**Stack:** Python 3.10+ · pandas · REST API · Airflow-ready
**Status:** Production (Hospital Viana)

A 3-stage ETL pipeline that ingests medical claims data via REST API,
validates against DQ rules, and produces a clean dataset for downstream
actuarial and financial reporting.

## The Problem

Hospital Viana needed a reliable, repeatable pipeline to ingest claims data
from a legacy system, clean and validate it, and deliver it to the finance
team — without manual intervention or data quality gaps that would cause
IFRS17 audit failures.

## The Solution

```
API (claims endpoint)
    │
    ▼
[Stage 1: Ingest]      requests.get → raw JSON → data/raw_claims.json
    │
    ▼
[Stage 2: Validate]   6 DQ checks (NOT NULL, RANGE, referential integrity)
                        → blocked on DQ failure, proceeds on warning
    │
    ▼
[Stage 3: Output]      clean CSV → data/claims_clean.csv
                        + quality report → data/dq_report.json
```

## Results

| Metric | Value |
|--------|-------|
| DQ checks | 6 (all documented in `data/quality_rules.md`) |
| Rejection threshold | Any blocker → pipeline halts, no silent failure |
| Stages | 3 (ingest / validate / report) |
| Trigger | Daily via GitHub Actions or manual |

## Tech

| Layer | Tool |
|-------|------|
| Ingestion | `requests` + raw JSON |
| Validation | Custom DQ rules (see `data/quality_rules.md`) |
| Output | CSV + JSON quality report |
| Scheduling | GitHub Actions (`.github/workflows/`) |
| Pipeline | 3-stage Python (`main.py`) |

## Setup

```bash
git clone https://github.com/laurentaf/hospital-viana-claims.git
cd hospital-viana-claims
python -m venv .venv && .venv\Scripts\activate   # Windows
# or: source .venv/bin/activate                    # Linux/macOS
pip install -r requirements.txt
python main.py
```

## What This Proves

| Skill | Evidence |
|-------|----------|
| API ingestion | REST API with auth token, multi-record JSON response |
| DQ validation | 6 rules, blocker vs warning separation, no silent failures |
| Healthcare domain | Claims data (sinistros), actuarial context |
| Production pipeline | GitHub Actions scheduling, SLAs, trigger documented |
| IFRS17-adjacent work | Clean claims data feeding financial reporting |

## Related

- ETL pattern: [giovanna-rupture-monitor](https://github.com/laurentaf/giovanna-rupture-monitor)
  (same 3-stage architecture, different domain)
- Dashboard: [abandono-academico-casa-grande](https://github.com/laurentaf/abandono-academico-casa-grande)
  (self-contained HTML dashboard, similar pattern)

---

Built with [LAOS](https://github.com/laurentaf/laos) — Laurent Agent Operating System.