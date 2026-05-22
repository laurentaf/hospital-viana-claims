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
    source = Path("airflow_sinistros/dag_sinistros.py").read_text()
    for mod in UNAPPROVED_UNUSED_IMPORTS:
        assert mod not in source or mod + "." not in source, f"Remove unused import {mod}"
```

---

## KB-003: Airflow DAG test patterns (static analysis)

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
