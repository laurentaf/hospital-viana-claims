# Knowledge Base — Airflow DAG Patterns

## KB-001: `retry_delay` must be `timedelta`, not int

**Severity:** MAJOR (runtime error)

Airflow's `default_args` expects `retry_delay` to be a `datetime.timedelta`, not a plain integer.

```python
# Wrong — Airflow crashes when calculating next retry
default_args = {
    "retries": 2,
    "retry_delay": 300,  # int — ERROR
}

# Correct
from datetime import timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),  # timedelta — OK
}
```

**Why:** Airflow uses `retry_delay` in arithmetic with `datetime` objects. An int has no `total_seconds()` or comparison semantics compatible with datetimes.

---

## KB-002: Remove unused imports

**Severity:** MINOR (linter noise)

Every import in a DAG file is parsed by Airflow's scheduler on every heartbeat. Unused imports waste CPU and cause linter warnings.

```python
# Wrong
import json       # never used
import os         # used
from datetime import datetime, timedelta

# Also wrong — direct `from datetime import datetime` without `timedelta`
# if you use timedelta in the code

# Correct — only what's needed
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
```

**Test pattern:** Use AST analysis to detect unused imports.
```python
UNAPPROVED_UNUSED_IMPORTS = {"json", "pickle", "csv"}

def test_no_unused_imports():
    unused = check_unused_imports(_parse())
    assert not unused, f"Remove unused imports: {unused}"
```

---

## KB-003: Project IDs and secrets must be env vars, not hardcoded

**Severity:** MAJOR (security + environment portability)

Never hardcode project IDs, API URLs with embedded IDs, or tokens in DAG files.

```python
# Wrong — hardcoded, cannot promote between environments
PROJECT_ID = "1b077a7a-b328-4546-8780-9a7ab909c152"
API_URL = f"https://api.datamission.com.br/projects/{PROJECT_ID}/dataset?format=csv"

# Correct — read from env var via Pydantic Settings
from src.core.config import settings

def _build_api_url() -> str:
    project_id = os.environ["DATAMISSION_PROJECT_ID"]  # or settings.DATAMISSION_PROJECT_ID
    return f"https://api.datamission.com.br/projects/{project_id}/dataset?format=csv"
```

**Test pattern:**
```python
def test_project_id_from_env_not_hardcoded():
    source = DAG_PATH.read_text()
    assert "DATAMISSION_PROJECT_ID" in source
    assert "1b077a7a" not in source or "env" in source
```

---

## KB-004: Use `logging` not `print()`

**Severity:** MEDIUM (observability)

Airflow captures `print()` output in task logs, but `logging` provides log levels (info/warning/error), structured output, and integration with Airflow's log system.

```python
# Wrong
print("Downloaded 1000 bytes")

# Correct
import logging
logger = logging.getLogger(__name__)
logger.info("Downloaded %d bytes", content_size)
```

**Test pattern:**
```python
def test_uses_logging_not_print():
    source = DAG_PATH.read_text()
    assert "logger." in source
    assert "print(" not in source
```

---

## KB-005: Reuse template's DataQualityValidator, don't reinvent validation

**Severity:** MEDIUM (maintainability)

The template provides 8 ready-made quality rules in `src/core/data_quality.py`. Use them instead of ad-hoc pandas checks.

```python
# Wrong — ad-hoc
if df["sinistro_id"].isnull().any():
    raise ValueError("Nulls found")

# Correct — uses template engine
from src.core.data_quality import DataQualityValidator, QualityCheck

validator = DataQualityValidator()
checks = [
    QualityCheck(column="sinistro_id", rule="not_null"),
    QualityCheck(column="sinistro_id", rule="unique"),
]
results = validator.check_table("sinistros_raw", checks)
```

**Test pattern:**
```python
def test_validate_uses_data_quality():
    source = DAG_PATH.read_text()
    assert "DataQualityValidator" in source
```

---

## KB-006: Airflow DAG test patterns (static analysis)

**Severity:** MAJOR (process)

Test DAGs without executing Airflow code using Python's `ast` module. This catches structural errors at `pytest` speed (no Airflow install needed).

**Key checks for every DAG:**
1. File exists
2. Required imports present (`requests`, `PythonOperator`, `DAG`)
3. `fetch_sinistros()` function exists with `requests.get` call
4. `PythonOperator` references the function
5. `default_args` contains `retry_delay` as `timedelta`
6. No unused imports (`json`, `pickle`, `csv`)
7. Docstring present with business context
8. Schedule defined (`@daily`, `@weekly`, etc.)
9. DAG id matches project naming convention

See `tests/test_dag_sinistros.py` for the full implementation.
