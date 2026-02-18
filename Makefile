.PHONY: help setup install run-extract run-load run-dbt run-quality run-all test clean format lint type-check dashboard

help:
	@echo "CDC Surveillance Pipeline - Available Commands"
	@echo "=============================================="
	@echo "  make setup          - Create virtual environment and install dependencies"
	@echo "  make install        - Install dependencies (assumes venv is active)"
	@echo "  make run-extract    - Run CDC data extraction"
	@echo "  make run-load       - Load data into DuckDB"
	@echo "  make run-dbt        - Run dbt transformations"
	@echo "  make run-quality    - Run data quality checks"
	@echo "  make run-all        - Run complete pipeline (extract → load → dbt → quality)"
	@echo "  make test           - Run pytest suite"
	@echo "  make format         - Format code with black"
	@echo "  make lint           - Lint code with ruff"
	@echo "  make type-check     - Type check with mypy"
	@echo "  make dashboard      - Launch Streamlit dashboard"
	@echo "  make clean          - Remove cache and build artifacts"

setup:
	python -m venv venv
	@echo "Virtual environment created. Activate with:"
	@echo "  Windows: venv\\Scripts\\activate"
	@echo "  macOS/Linux: source venv/bin/activate"
	@echo "Then run: make install"

install:
	pip install --upgrade pip
	pip install poetry
	poetry install

run-extract:
	python -m src.extract.cdc_api_client

run-load:
	python -m src.load.duckdb_loader

run-dbt:
	cd dbt_project && dbt run && dbt test

run-quality:
	python -m data_quality.quality_checks

run-all: run-extract run-load run-dbt run-quality
	@echo "✓ Pipeline complete!"

test:
	pytest -v

format:
	black src/ tests/ data_quality/ dashboard/

lint:
	ruff check src/ tests/ data_quality/ dashboard/

type-check:
	mypy src/

dashboard:
	streamlit run dashboard/app.py

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/
