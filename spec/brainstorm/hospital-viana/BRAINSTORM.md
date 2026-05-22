# Brainstorm: Pipeline de Ingestão e Validação de Sinistros Médicos

## Contexto
Hospital Viana — R$ 300M receita anual. Sem pipeline orquestrado, dados desatualizados levam a auditorias falhas, pagamentos indevidos e multas. Necessidade de pipeline que consuma dados crus de sinistros via API, valide e exponha para análise de risco antes do fechamento mensal.

## Approach 1 — Airflow DAG puro com PythonOperator
**Como funciona:** DAG Airflow com tarefas sequenciais: HTTP GET → CSV local → validação Pandas → staging DuckDB
**Prós:** Simples, baixo acoplamento, sem dependências externas
**Contras:** Sem schema evolution tracking, sem linhagem automática, sem qualidade reutilizável
**Custo:** Zero (Airflow + DuckDB são gratis)

## Approach 2 — Airflow + Medallion (Bronze/Silver/Gold)
**Como funciona:** Adota o padrão LTADE Medallion: Bronze guarda raw, Silver limpa e tipa, Gold expõe para análise. Cada camada é uma tarefa Airflow.
**Prós:** Separação clara de responsabilidades, dados raw preservados, qualidade aplicada na transição Silver
**Contras:** Mais tarefas no DAG, maior consumo de disco
**Custo:** Zero

## Approach 3 — Airflow + OpenLineage + Data Quality
**Como funciona:** Approach 2 + OpenLineage para linhagem + testes de qualidade (not null, unique, range) em cada camada
**Prós:** Auditável, rastreável, compliance-ready
**Contras:** Complexidade adicional, OpenLineage precisa de backend (Marquez ou similar)
**Custo:** Backend OpenLineage requer infra

## Questions

### Question 1: Qual o volume esperado de dados?
Dados de sinistros médicos de hospital com R$ 300M receita — estimativa de 50k-200k registros/mês. CSV via API. Volume pequeno, DuckDB é suficiente.

### Question 2: Precisa de linhagem e auditoria?
Sim — contexto de pagamentos indevidos e multas. Approach 3 (OpenLineage) é desejável para produção, mas Approach 2 é suficiente para MVP.

### Question 3: Onde o pipeline será executado?
Ambiente Windows com Docker (Airflow em container) ou Airflow standalone local. Sem cloud definida.

### Question 4: Qual a frequência de execução?
Diária, com fechamento mensal crítico. DAG com schedule diário e trigger manual para reprocessamento.

### Question 5: Quem consumirá os dados validados?
Equipe de risco — precisa de tabelas Silver/Gold limpas para análise. Possivelmente Power BI ou Metabase.

## YAGNI Filter
**Out of scope (fora de escopo — não fazer agora):**
- Streaming em tempo real
- ML/models preditivos
- Dashboard em tempo real
- Multi-cloud
- Data lake em S3/Blob

**Dentro de escopo:**
- DAG Airflow funcional
- Download via API com autenticação
- Validação básica (nulos, duplicatas, tipos, ranges)
- Tabelas Bronze (raw), Silver (limpa), Gold (analítica)
- Documentação do schema
- Testes unitários

## Success Criteria
1. DAG executa sem erros com dados reais da API
2. Dados raw preservados na camada Bronze
3. Validações detectam registros com nulos/duplicatas
4. Camada Gold expõe dados prontos para análise
5. Pipeline roda em < 10 minutos para um mês de dados
