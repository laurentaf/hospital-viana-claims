# DESIGN: Pipeline de Ingestão e Validação de Sinistros Médicos

## File Manifest
```
airflow_sinistros/
├── dag_sinistros.py           # DAG principal (fetch + validate)
├── requirements.txt           # apache-airflow, pandas, requests
data/
├── raw/                       # CSV particionado por data
├── staging/                   # DuckDB validado (sinistros_validados.duckdb)
src/core/
├── config.py                  # Settings Pydantic (DATAMISSION_*)
├── data_quality.py            # 8 regras de validação (template)
spec/
├── brainstorm/hospital-viana/BRAINSTORM.md
├── define/dag-sinistros/DEFINE.md
├── design/dag-sinistros/DESIGN.md
tests/
├── test_core.py               # Testes do template (37)
├── test_dag_sinistros.py      # Testes do DAG (18+)
docs/
├── decisions.json             # ADR-001, ADR-002, ADR-003
├── knowledge_base.md          # KB-001, KB-002, KB-003
```

## Data Flow Diagram
```
[DataMission API]
       |
       | GET /projects/{id}/dataset?format=csv
       | Bearer token
       v
[t1: fetch_sinistros]  ──>  data/raw/sinistros_{ds}.csv
       |
       | XCom: raw_path
       v
[t2: validate_sinistros]
       |
       | 1. Ler CSV com pandas
       | 2. Checar colunas obrigatórias (sinistro_id, valor)
       | 3. Quality checks (not_null, unique, positive)
       | 4. Salvar DuckDB staging
       v
[data/staging/sinistros_validados.duckdb]
       |
       v
[Equipe de Risco → Power BI / Metabase]
```

## Schema Design

### Raw CSV (da API)
| Coluna | Tipo Esperado | Obrigatório |
|--------|--------------|-------------|
| sinistro_id | string | sim |
| valor | float | sim |
| ... (demais colunas da API) | variado | não |

### Quality Checks
| Coluna | Regra | Parâmetros |
|--------|-------|-----------|
| sinistro_id | not_null | — |
| sinistro_id | unique | — |
| valor | not_null | — |
| valor | positive | — |

## ADRs Documentados
- **ADR-001**: Airflow como orquestrador (vs Prefect)
- **ADR-002**: PROJECT_ID como env var (vs hardcoded)
- **ADR-003**: DataQualityValidator do template (vs pandas ad-hoc)

## IDEMPOTÊNCIA
O DAG é idempotente: cada execução diária gera `sinistros_{ds}.csv` distinto.
Reprocessamento de um mesmo dia sobrescreve o arquivo daquela data.
