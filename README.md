# CDC Surveillance Data Pipeline

A production-grade data engineering project that ingests CDC disease surveillance data, transforms it with dbt, and generates automated insights.

## Quick Start

```bash
# 1. Create virtual environment and install
make setup && source venv/bin/activate && make install

# 2. Run the entire pipeline
make run-all

# 3. View the dashboard
make dashboard
```

## What's Inside

- **Extract**: CDC API client pulls COVID-19 and Influenza data
- **Load**: DuckDB data warehouse with raw data tables
- **Transform**: dbt models for staging, intermediate, and analytical layers
- **Validate**: Automated data quality checks
- **Visualize**: Streamlit dashboard with key metrics
- **Automate**: GitHub Actions for daily scheduled runs

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Extraction | Python + requests |
| Storage | DuckDB |
| Transformation | dbt + DuckDB |
| Quality | Python |
| Dashboard | Streamlit |
| Orchestration | GitHub Actions |
| Dependencies | Poetry |

## Available Commands

```bash
make help              # Show all commands
make run-all           # Extract → Load → Transform → Validate
make run-extract       # Extract CDC data from APIs
make run-load          # Load into DuckDB
make run-dbt           # Run dbt transformations
make run-quality       # Run data quality checks
make dashboard         # Launch Streamlit dashboard
make test              # Run pytest suite
make format            # Format code with black
make lint              # Lint with ruff
```

## Project Structure

```
├── src/                      # Extraction, loading, utilities
│   ├── extract/cdc_api_client.py
│   └── load/duckdb_loader.py
├── dbt_project/              # dbt transformation models
│   ├── models/staging/       # Clean raw data
│   ├── models/intermediate/  # Aggregations & metrics
│   └── models/marts/         # Final analytical models
├── dashboard/app.py          # Streamlit dashboard
├── data_quality/quality_checks.py  # Quality validation
├── tests/                    # Unit tests
├── Makefile                  # Common commands
├── pyproject.toml            # Poetry configuration
└── README.md                 # This file
```

## Pipeline Flow

```
CDC APIs
   ↓
Extract (JSON files)
   ↓
DuckDB (raw schema)
   ↓
dbt (staging → intermediate → marts)
   ↓
Quality Checks
   ↓
Streamlit Dashboard
```

## Getting Help

1. Check the troubleshooting section in detailed docs
2. Review logs in the console output
3. Run `make help` for all available commands
4. Check individual module docstrings

---

**Built as a portfolio project demonstrating modern data engineering practices with Python, dbt, and DuckDB.**