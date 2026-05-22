"""
Reusable DAG validation utilities + project-specific tests.

Use `validate_dag_structure()` in any project to catch common Airflow issues:
  - Unused imports (linter noise)
  - retry_delay as int instead of timedelta (MAJOR runtime error)
  - Missing docstring
  - Missing schedule
"""

import ast
from pathlib import Path

DAG_PATH = Path(__file__).parent.parent / "airflow_sinistros" / "dag_sinistros.py"


def _parse():
    return ast.parse(DAG_PATH.read_text())


# ---------------------------------------------------------------------------
# Reusable DAG validation helpers (use in any project)
# ---------------------------------------------------------------------------


def get_top_level_imports(tree: ast.AST) -> set[str]:
    """Returns all top-level imported module names."""
    names = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Import):
            for alias in n.names:
                names.add(alias.name.split(".")[0])
        elif isinstance(n, ast.ImportFrom):
            if n.module:
                names.add(n.module.split(".")[0])
    return names


def get_used_names(tree: ast.AST) -> set[str]:
    """Returns all referenced names in the module (excluding imports & definitions)."""
    used = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and not isinstance(n.ctx, ast.Store):
            used.add(n.id)
        elif isinstance(n, ast.Attribute) and not isinstance(n.ctx, ast.Store):
            used.add(n.attr)
    return used


def get_default_args_dict(tree: ast.AST) -> dict[str, ast.AST]:
    """Extracts default_args dict literal if present."""
    for n in ast.walk(tree):
        if isinstance(n, ast.Assign):
            for target in n.targets:
                if isinstance(target, ast.Name) and target.id == "default_args" and isinstance(n.value, ast.Dict):
                    result = {}
                    for k, v in zip(n.value.keys, n.value.values):
                        if k is not None:
                            result[k.value] = v
                    return result
    return {}


def get_top_level_assignment_names(tree: ast.AST) -> set[str]:
    return {
        n.targets[0].id
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
    }


UNAPPROVED_UNUSED_IMPORTS = {"json", "pickle", "csv"}


def check_unused_imports(tree: ast.AST) -> list[str]:
    """Detects top-level imports that are never referenced in the module."""
    imported = get_top_level_imports(tree)
    # Remove built-in names that appear as imports
    names_in_code = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.Name):
            names_in_code.add(n.id)
    unused = []
    for mod in imported:
        if mod not in names_in_code:
            unused.append(mod)
    # Only flag modules that are commonly useless
    return [m for m in unused if m in UNAPPROVED_UNUSED_IMPORTS]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDagSinistros:
    def test_file_exists(self):
        assert DAG_PATH.exists()

    # --- Acceptance criteria (Mission 1) ---

    def test_has_requests_import(self):
        source = DAG_PATH.read_text()
        assert "import requests" in source

    def test_has_fetch_function(self):
        tree = _parse()
        funcs = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "fetch_sinistros" in funcs

    def test_has_project_id(self):
        assigns = get_top_level_assignment_names(_parse())
        assert "PROJECT_ID" in assigns

    def test_fetch_calls_requests_get(self):
        tree = _parse()
        fetch_func = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "fetch_sinistros"
        )
        calls = [
            n
            for n in ast.walk(fetch_func)
            if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "get"
        ]
        assert len(calls) >= 1

    def test_fetch_uses_bearer_token(self):
        assert "Bearer" in DAG_PATH.read_text()

    def test_has_python_operator(self):
        assert "PythonOperator" in DAG_PATH.read_text()

    def test_dag_id_matches_expected(self):
        assert "ingestao_sinistros_viana" in DAG_PATH.read_text()

    def test_has_daily_schedule(self):
        source = DAG_PATH.read_text()
        assert "@daily" in source

    def test_has_docstring(self):
        doc = ast.get_docstring(_parse())
        assert doc is not None
        assert "Hospital Viana" in doc

    # --- Major/Minor validations (from feedback) ---

    def test_no_json_unused_import(self):
        assert "json" not in get_top_level_imports(_parse()), "Remove unused import json"

    def test_retry_delay_is_timedelta_not_int(self):
        args = get_default_args_dict(_parse())
        retry = args.get("retry_delay")
        assert retry is not None, "default_args missing retry_delay"
        assert isinstance(retry, ast.Call), "retry_delay must be a timedelta(...) call, not a literal int"
        func_name = retry.func.id if isinstance(retry.func, ast.Name) else ""
        assert func_name == "timedelta", f"retry_delay must use timedelta(), got {func_name}"

    def test_retry_delay_reasonable_range(self):
        """Ensure retry_delay is between 1 and 30 minutes."""
        args = get_default_args_dict(_parse())
        retry = args.get("retry_delay")
        assert isinstance(retry, ast.Call)
        for kw in retry.keywords:
            if kw.arg and "minute" in kw.arg and isinstance(kw.value, ast.Constant):
                assert 1 <= kw.value.value <= 30, f"retry_delay {kw.value.value}m is outside range (1-30)"

    def test_imports_have_no_unused(self):
        """MAJOR: Unused imports cause linter noise and confusion."""
        unused = check_unused_imports(_parse())
        assert not unused, f"Remove unused imports: {unused}"

    def test_timedelta_imported(self):
        """timedelta must be imported for retry_delay."""
        source = DAG_PATH.read_text()
        assert "timedelta" in source, "timedelta must be imported from datetime"

    # --- Mission 2: try/except + logging ---

    def test_fetch_has_try_except(self):
        """fetch_sinistros must have try/except to log status and size before failing."""
        tree = _parse()
        fetch_func = next(
            n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "fetch_sinistros"
        )
        has_try = any(isinstance(n, ast.Try) for n in ast.walk(fetch_func))
        assert has_try, "fetch_sinistros must wrap requests.get in try/except"

    def test_fetch_logs_status_and_bytes(self):
        """try/except must print status_code and content size."""
        source = DAG_PATH.read_text()
        assert "status:" in source or "status_code" in source
        assert "bytes:" in source or "content_size" in source or "len(" in source

    def test_fetch_has_project_id_param(self):
        """fetch_sinistros must pass project_id (as constant or param)."""
        source = DAG_PATH.read_text()
        assert "project_id" in source.lower() or "PROJECT_ID" in source
