.PHONY: help install install-uv install-pip test clean run output

# Default target
help:
	@echo "Data Quality Monitoring - Available Commands:"
	@echo ""
	@echo "  make install       - Install dependencies using uv (recommended)"
	@echo "  make install-pip   - Install dependencies using pip"
	@echo "  make run           - Run data quality analysis"
	@echo "  make test          - Unit tests"
	@echo "  make test-coverage - Unit tests with coverage"
	@echo "  make clean         - Clean output files and cache"
	@echo "  make output        - Show latest results"
	@echo "  make help          - Show this help message"
	@echo ""

# Installation targets
install:
	@echo "Installing dependencies with uv..."
	uv sync --extra dev
	@echo "✓ Dependencies installed.  -> Please Activate with: source .venv/bin/activate"

install-pip:
	@echo "Installing dependencies with pip..."
	pip install -r requirements.txt
	@echo "✓ Dependencies installed"

# Run targets
run:
	@echo "Running data quality analysis..."
	python scripts/run_dq_analysis.py --data test_data/grant_applications.csv --output output
	@echo ""
	@echo "✓ Analysis complete. Results in output/"

# Test targets
test:
	@echo "Running unit tests..."
	@if [ -d ".venv" ]; then \
		.venv/bin/python3 -m pytest tests/ -v; \
	else \
		python3 -m pytest tests/ -v; \
	fi
	@echo ""
	@echo "✓ Tests complete"

test-coverage:
	@echo "Running unit tests with coverage..."
	@if [ -d ".venv" ]; then \
		.venv/bin/python3 -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term; \
	else \
		python3 -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term; \
	fi
	@echo ""
	@echo "✓ Tests complete with coverage"

# Utility targets
output:
	@echo "Latest dimension scores:"
	@echo ""
	@if [ -f "output/dq_dimension_scores/part-00000"*.csv ]; then \
		cat output/dq_dimension_scores/part-00000*.csv 2>/dev/null | head -10; \
	else \
		echo "No results found. Run 'make test' first."; \
	fi

clean:
	@echo "Cleaning output files and cache..."
	rm -rf output/*
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache
	rm -rf __pycache__
	find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Clean complete"

