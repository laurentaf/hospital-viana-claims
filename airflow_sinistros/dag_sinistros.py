"""
DAG: ingestao_sinistros_viana

Pipeline de ingestão e validação de sinistros médicos do Hospital Viana.

Consome dados crus via API da DataMission, salva CSV localmente,
aplica verificações básicas de qualidade e expõe registros validados
para análise da equipe de risco.

Business context:
  Hospital Viana — R$ 300M receita anual. Dados desatualizados levam
  a auditorias falhas, pagamentos indevidos e multas. Este DAG garante
  que a equipe de risco tenha dados confiáveis antes do fechamento mensal.
"""

import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ID = "1b077a7a-b328-4546-8780-9a7ab909c152"
API_URL = f"https://api.datamission.com.br/projects/{PROJECT_ID}/dataset?format=csv"
RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
STAGING_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "staging")

default_args = {
    "owner": "hospital-viana",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def fetch_sinistros(**context):
    token = os.environ.get("DATAMISSION_API_KEY")
    if not token:
        raise ValueError("DATAMISSION_API_KEY environment variable not set")

    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(API_URL, headers=headers, timeout=120)
        status = response.status_code
        content_size = len(response.content)
        print(f"API response — status: {status}, bytes: {content_size}")
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"API request failed — status: {status}, bytes: {content_size}, error: {e}")
        raise

    os.makedirs(RAW_DIR, exist_ok=True)
    raw_path = os.path.join(RAW_DIR, "sinistros.csv")
    with open(raw_path, "wb") as f:
        f.write(response.content)

    context["ti"].xcom_push(key="raw_path", value=raw_path)
    context["ti"].xcom_push(key="raw_bytes", value=content_size)
    context["ti"].xcom_push(key="status_code", value=status)


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
        provide_context=True,
    )
