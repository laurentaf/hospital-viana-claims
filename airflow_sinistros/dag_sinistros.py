"""
DAG: ingestao_sinistros_viana

Pipeline de ingestão e validação de sinistros médicos do Hospital Viana.

Consome dados crus via API da DataMission, salva CSV em data/raw/sinistros.csv,
aplica verificações de qualidade (schema + regras) e expõe registros
validados para análise da equipe de risco.

Business context:
  Hospital Viana — R$ 300M receita anual. Dados desatualizados levam
  a auditorias falhas, pagamentos indevidos e multas. Este DAG garante
  que a equipe de risco tenha dados confiáveis antes do fechamento mensal.
"""

import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.core.config import settings
from src.core.data_quality import DataQualityValidator, QualityCheck

logger = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")

default_args = {
    "owner": "hospital-viana",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _build_api_url(project_id: str) -> str:
    return f"https://api.datamission.com.br/projects/{project_id}/dataset?format=csv"


def fetch_sinistros(project_id, **context):
    token = settings.DATAMISSION_API_KEY
    if not token:
        raise ValueError("DATAMISSION_API_KEY environment variable not set")

    url = _build_api_url(project_id)
    headers = {"Authorization": f"Bearer {token}"}

    status = 0
    content_size = 0
    try:
        response = requests.get(url, headers=headers, timeout=120)
        status = response.status_code
        content_size = len(response.content)
        logger.info("API response — status: %d, bytes: %d", status, content_size)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("API request failed — status: %d, bytes: %d, error: %s", status, content_size, e)
        raise

    os.makedirs(RAW_DIR, exist_ok=True)
    raw_path = os.path.join(RAW_DIR, "sinistros.csv")
    with open(raw_path, "wb") as f:
        f.write(response.content)

    context["ti"].xcom_push(key="raw_path", value=raw_path)
    context["ti"].xcom_push(key="raw_bytes", value=content_size)
    context["ti"].xcom_push(key="status_code", value=status)
    logger.info("Saved %d bytes to %s", content_size, raw_path)


def validate_sinistros(**context):
    ti = context["ti"]
    raw_path = ti.xcom_pull(key="raw_path", task_ids="fetch_sinistros")

    if not raw_path or not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    logger.info("Validating %s", raw_path)
    df = pd.read_csv(raw_path)
    logger.info("Loaded %d rows, %d columns", len(df), len(df.columns))

    required_cols = {"sinistro_id", "valor"}
    actual_cols = set(df.columns)
    missing = required_cols - actual_cols
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    validator = DataQualityValidator()
    db_path = os.path.join(os.path.dirname(RAW_DIR), "staging", "sinistros_validados.duckdb")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    df.to_sql("sinistros_raw", validator.con, if_exists="replace", index=False)

    checks = [
        QualityCheck(column="sinistro_id", rule="not_null"),
        QualityCheck(column="sinistro_id", rule="unique"),
        QualityCheck(column="valor", rule="not_null"),
        QualityCheck(column="valor", rule="positive"),
    ]
    results = validator.check_table("sinistros_raw", checks)
    report = validator.report(results)
    logger.info("Quality report:\n%s", report)

    validator.con.execute(f"CREATE OR REPLACE TABLE sinistros_clean AS SELECT * FROM sinistros_raw")
    validator.con.execute(f"ATTACH '{db_path}' AS staging")
    validator.con.execute("CREATE OR REPLACE TABLE staging.sinistros AS SELECT * FROM sinistros_raw")
    validator.close()

    passed = all(r.passed for r in results)
    ti.xcom_push(key="validation_passed", value=passed)
    ti.xcom_push(key="total_rows", value=len(df))
    ti.xcom_push(key="quality_report", value=report)

    if not passed:
        raise ValueError(f"Quality checks failed:\n{report}")

    logger.info("Validation passed — %d rows clean", len(df))


with DAG(
    dag_id="ingestao_sinistros_viana",
    default_args=default_args,
    description="Pipeline de ingestão e validação de sinistros médicos — Hospital Viana",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hospital-viana", "sinistros", "datamission"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_sinistros",
        python_callable=fetch_sinistros,
        op_kwargs={"project_id": settings.DATAMISSION_PROJECT_ID or os.environ.get("DATAMISSION_PROJECT_ID")},
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id="validate_sinistros",
        python_callable=validate_sinistros,
        provide_context=True,
    )

    t1 >> t2
