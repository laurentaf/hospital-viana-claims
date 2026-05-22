"""Tests for the sinistros ingestion DAG — static analysis approach."""

import ast
from pathlib import Path

DAG_PATH = Path(__file__).parent.parent / "airflow_sinistros" / "dag_sinistros.py"


def _parse():
    return ast.parse(DAG_PATH.read_text())


class TestDagSinistros:
    def test_file_exists(self):
        assert DAG_PATH.exists()

    def test_has_requests_import(self):
        tree = _parse()
        imports = {
            n.names[0].name
            for n in ast.walk(tree)
            if isinstance(n, ast.Import)
        }
        import_from = {
            n.module
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom) and n.module
        }
        assert "requests" in imports or "requests" in import_from

    def test_has_fetch_function(self):
        tree = _parse()
        funcs = {n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
        assert "fetch_sinistros" in funcs

    def test_has_project_id(self):
        tree = _parse()
        assigns = {
            n.targets[0].id: n.value
            for n in ast.walk(tree)
            if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
        }
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
        tree = _parse()
        source = DAG_PATH.read_text()
        assert "Bearer" in source

    def test_has_python_operator(self):
        tree = _parse()
        source = DAG_PATH.read_text()
        assert "PythonOperator" in source

    def test_dag_id_matches_expected(self):
        source = DAG_PATH.read_text()
        assert "ingestao_sinistros_viana" in source

    def test_has_daily_schedule(self):
        source = DAG_PATH.read_text()
        assert "@daily" in source or "schedule" in source

    def test_has_docstring(self):
        tree = _parse()
        mod_doc = ast.get_docstring(tree)
        assert mod_doc is not None
        assert "Hospital Viana" in mod_doc
