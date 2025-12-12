# 🇧🇷 Orquestrador ETL: Dados Municipais (Airflow + Polars)

## 🎯 Resumo do Projeto

Este projeto é um *pipeline* de processamento de dados robusto e auto-contido, orquestrado pelo **Apache Airflow** e rodando em **Docker**.

Seu objetivo principal é garantir a **carga segura** e **enriquecida** de dados municipais (simulando indicadores ou movimentações) em um banco de dados **PostgreSQL**.

## ✨ O Que Resolve?

| Aspecto | Solução |
| :--- | :--- |
| **Deduplicação / Idempotência** | Garante que o *pipeline* possa ser reexecutado múltiplas vezes sem duplicar dados, usando a lógica *Delete & Insert* baseada em (Município, Mês e Ano). |
| **Performance** | Utiliza a biblioteca **Polars** (em vez de apenas Pandas) para transformações e *Joins* ultrarrápidos, enriquecendo os dados com o `codigo_ibge`. |
| **Rastreabilidade** | Adiciona colunas `id` (chave primária) e `data_processamento` ao banco, permitindo auditoria completa sobre o momento da inserção. |
| **Setup** | Containerizado via **Docker Compose**, permitindo que o ambiente completo (Airflow, Postgres e Polars) seja iniciado com um único comando (`docker-compose up`). |

---


## 📊 ETL Orchestrator: Municipal Data (Airflow + Polars)

## 🎯 Project Summary

This project is a robust and self-contained data processing pipeline, orchestrated by **Apache Airflow** and running on **Docker**.

Its primary goal is to ensure the **safe and enriched loading** of municipal data (simulating indicators or movements) into a **PostgreSQL** database.

## ✨ Key Benefits

| Aspect | Solution |
| :--- | :--- |
| **Deduplication / Idempotency** | Ensures the pipeline can be run multiple times without duplicating data, using *Delete & Insert* logic based on (Município, Month, and Year). |
| **Performance** | Uses the **Polars** library (instead of just Pandas) for ultra-fast transformations and Joins, enriching data with the `codigo_ibge`. |
| **Traceability** | Adds `id` (primary key) and `data_processamento` columns to the database, allowing complete auditing of the insertion time. |
| **Setup** | Containerized via **Docker Compose**, allowing the entire environment (Airflow, Postgres, and Polars) to be launched with a single command (`docker-compose up`). |
