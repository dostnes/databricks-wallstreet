# 📈 Project WallStreet: Databricks Stock Ingestion

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![Databricks](https://img.shields.io/badge/databricks-runtime-orange)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)

## 📖 Om Prosjektet
Dette er en **Hybrid ELT-pipeline** designet for å hente finansiell data ("The Magnificent Seven" + OSEBX) og ingeste dette til **Databricks Bronze Layer**.

Prosjektet demonstrerer moderne "Best Practices" for Databricks-utvikling, inkludert:
* **Programmatisk avhengighetshåndtering:** Scriptet sjekker og installerer `yfinance` automatisk ved kjøring.
* **Governance & Code Quality:** Bruker **Ruff** for linting/formatering og **Pre-commit hooks** for å sikre kodestandard før opplasting.
* **Datakvalitet:** Håndterer Yahoo Finance API data for å få det inn i Databricks.

## 🛠️ Teknologistack
* **Compute:** Databricks (Community Edition)
* **Ingestion:** Python (`yfinance`, `pandas`)
* **Processing:** PySpark (Delta Lake)
* **Code Quality:** Ruff, Pre-commit

## 🚀 Hvordan kjøre koden
1. **Clone repoet** inn i Databricks Repos.
2. Åpne `src/ingest_stocks.py`.
3. Klikk **Run**.
4. Data blir tilgjengelig i `wallstreet_bronze.raw_stocks`.

## 🛡️ Lokal Utvikling (WSL)
For å bidra til prosjektet lokalt:

1. **Installer dependencies:**
   ```bash
   pip install -r requirements.txt
