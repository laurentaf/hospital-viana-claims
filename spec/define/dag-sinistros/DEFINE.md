# DEFINE: Pipeline de Ingestão e Validação de Sinistros Médicos

## Problem Scope
Hospital Viana não possui pipeline orquestrado para ingestão de sinistros médicos. Dados crus chegam via API da DataMission mas não há validação automática, versionamento ou rastreamento de qualidade. Isso leva a auditorias falhas, pagamentos indevidos e multas regulatórias.

## Success Metrics
| Métrica | Alvo | Como medir |
|---------|------|------------|
| Tempo de pipeline | < 10 min para 1 mês de dados | Airflow duration log |
| Dados validados | 100% dos registros passam quality checks | `validate_sinistros` task |
| Detecção de falhas | 100% das falhas HTTP logadas | Airflow log + retry |
| Cobertura de testes | 18+ testes passando | `pytest tests/` |

## Stakeholders
- **Equipe de Risco** — Consumidora dos dados validados (Power BI/Metabase)
- **Auditoria Interna** — Precisa de linhagem e rastreabilidade
- **Financeiro** — Fechamento mensal depende de dados confiáveis (R$ 300M)
- **TI/Dados** — Mantém o pipeline e a infraestrutura

## Constraints
- Airflow 2.10+ como orquestrador (padrão da organização)
- DuckDB para staging (sem custo de infra)
- API DataMission com autenticação Bearer token
- Sem cloud definida — execução local/Docker

## Dependencies
| Recurso | Tipo | Configuração |
|---------|------|-------------|
| DataMission API | Externa | `DATAMISSION_API_KEY` + `DATAMISSION_PROJECT_ID` |
| Airflow | Infra | 2.10+, scheduler + webserver |
| DuckDB | Local | Via `duckdb` + `pandas` |
| Python | Runtime | 3.11+, pacotes em `requirements.txt` |

## Risks
| Risco | Probabilidade | Mitigação |
|-------|--------------|-----------|
| API fora do ar | Baixa | 2 retentativas com 5 min delay + alerta |
| Esquema da API muda | Média | Validação de colunas obrigatórias no DAG |
| Dados corrompidos | Baixa | Quality checks (not_null, unique, positive) |
