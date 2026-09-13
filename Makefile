# ==============================================================================
# DATA ENGINEERING PLATFORM AUTOMATION MAKEFILE
# ==============================================================================

.PHONY: help setup lint format test generate-fixtures run-m1 clean

help:
	@echo "Available commands:"
	@echo "  make setup              - Install python dependencies in current environment"
	@echo "  make lint               - Run Ruff and Mypy checks"
	@echo "  make format             - Format code using Ruff"
	@echo "  make test               - Execute pytest suite"
	@echo "  make generate-fixtures  - Generate 100k stress and 50-row unit test fixtures"
	@echo "  make run-m1             - Execute Milestone 01 ingestion engine (DuckDB mode)"
	@echo "  make clean              - Remove temporary caches and local DB artifacts"

setup:
	pip install -e .[dev]

lint:
	python -m ruff check .
	python -m mypy src/ tests/

format:
	python -m ruff format .
	python -m ruff check --fix .

test:
	python -m pytest -v tests/

generate-fixtures:
	python -m src.milestone_01_baseline_scripted_ingestion.generate_sample_data

run-m1:
	python -m src.milestone_01_baseline_scripted_ingestion.pipeline --target duckdb --file data/unit_test_50.csv

run-m3-lakehouse:
	python -c "import asyncio; from src.milestone_03_warehouse_modular_mart.extract_api import MarketApiExtractor; from src.milestone_03_warehouse_modular_mart.parquet_writer import ParquetLakeWriter; trades = asyncio.run(MarketApiExtractor().extract_all(limit_per_symbol=500)); ParquetLakeWriter().write_partitioned_trades(trades)"

run-m3-transform:
	python -m src.milestone_03_warehouse_modular_mart.run_transformations

run-m3-benchmark:
	python -m src.milestone_03_warehouse_modular_mart.benchmark

run-m4-cdc:
	python -m src.milestone_04_lakehouse_cdc_contracts.run_cdc_lakehouse

run-m4-semantic:
	python -m src.milestone_04_lakehouse_cdc_contracts.semantic_layer

run-m5-stream:
	python -m src.milestone_05_resilient_streaming_platform.run_streaming_pipeline

run-m5-backfill:
	python -m src.milestone_05_resilient_streaming_platform.backfill --dry-run

run-dashboard:
	streamlit run src/dashboard/app.py --server.port 8501 --server.headless true

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	rm -rf data/*.duckdb*
	rm -rf data/lakehouse/
	rm -rf data/benchmark/


