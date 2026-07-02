.PHONY: help lint type-check test test-unit test-integration check-all clean bundle-validate bundle-deploy-dev

PYTHON := python
ENV ?= dev

help:
	@echo "DataGuardian - Available Commands"
	@echo ""
	@echo "  Development"
	@echo "    make lint              Run ruff linter"
	@echo "    make type-check        Run mypy type checker"
	@echo "    make test              Run full test suite with coverage"
	@echo "    make test-unit         Run unit tests only"
	@echo "    make test-integration  Run integration tests only"
	@echo "    make check-all         Run lint + type-check + test"
	@echo ""
	@echo "  Databricks"
	@echo "    make bundle-validate   Validate DAB bundle for ENV (default: dev)"
	@echo "    make bundle-deploy-dev Deploy bundle to DEV environment"
	@echo ""
	@echo "  Utilities"
	@echo "    make validate-config   Validate all YAML configuration files"
	@echo "    make clean             Remove build artifacts and caches"

lint:
	ruff check src/ tests/

type-check:
	mypy src/

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v -m unit

test-integration:
	pytest tests/integration/ -v -m integration

check-all: lint type-check test

validate-config:
	$(PYTHON) scripts/validate_config.py --env $(ENV)

bundle-validate:
	databricks bundle validate --target $(ENV)

bundle-deploy-dev:
	databricks bundle deploy --target dev

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	find . -name ".coverage" -delete 2>/dev/null || true
