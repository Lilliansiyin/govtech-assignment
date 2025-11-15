.PHONY: help install run test test-coverage clean docker-build docker-run docker-clean lint check-format check-imports

# Variables
PIPELINE_DIR := pipeline
CSV_PATH := test_data/individual_tax_returns.csv
PYTHON := python3
PYTEST := pytest

# Default target
help:
	@echo "Tax Data Pipeline - Available Commands:"
	@echo ""
	@echo "  make install          - Install Python dependencies"
	@echo "  make run              - Run the pipeline with test data"
	@echo "  make test             - Run unit tests"
	@echo "  make test-coverage    - Run tests with coverage report"
	@echo "  make clean            - Clean generated data files"
	@echo "  make docker-build     - Build Docker image"
	@echo "  make docker-run       - Run pipeline in Docker"
	@echo "  make docker-clean     - Remove Docker image"
	@echo "  make lint             - Format code with black and isort"
	@echo "  make check-format     - Check code formatting"
	@echo "  make check-imports    - Check import sorting"
	@echo ""

# Install dependencies
install:
	@echo "Installing dependencies..."
	cd $(PIPELINE_DIR) && $(PYTHON) -m pip install -r requirements.txt
	@echo "Installation complete!"

# Run pipeline
run:
	@echo "Running tax data pipeline..."
	cd $(PIPELINE_DIR) && $(PYTHON) main.py \
		--csv-path ../$(CSV_PATH)

# Run tests
test:
	@echo "Running unit tests..."
	cd $(PIPELINE_DIR) && python3 -m pytest tests/ -v

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	cd $(PIPELINE_DIR) && python3 -m pytest tests/ -v --cov=src --cov-report=html --cov-report=term

# Clean generated data
clean:
	@echo "Cleaning generated data files..."
	rm -rf test_output/
	rm -rf $(PIPELINE_DIR)/data/
	rm -rf $(PIPELINE_DIR)/warehouse/
	rm -rf $(PIPELINE_DIR)/__pycache__/
	rm -rf $(PIPELINE_DIR)/src/**/__pycache__/
	rm -rf $(PIPELINE_DIR)/tests/**/__pycache__/
	rm -rf $(PIPELINE_DIR)/*.pyc
	rm -rf $(PIPELINE_DIR)/.pytest_cache/
	rm -rf $(PIPELINE_DIR)/htmlcov/
	rm -rf $(PIPELINE_DIR)/.coverage
	rm -rf $(PIPELINE_DIR)/coverage.xml
	rm -rf $(PIPELINE_DIR)/.coverage.*
	find $(PIPELINE_DIR) -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null || true
	find $(PIPELINE_DIR) -type f -name "*.pyc" -delete 2>/dev/null || true
	find $(PIPELINE_DIR) -type d -name ".pytest_cache" -exec rm -r {} + 2>/dev/null || true
	@echo "Cleanup complete!"

# Docker commands
docker-build:
	@echo "Building Docker image..."
	cd $(PIPELINE_DIR) && docker build -t tax-pipeline .

docker-run:
	@echo "Running pipeline in Docker..."
	docker run --rm -v $(PWD)/test_data:/data tax-pipeline --csv-path /data/individual_tax_returns.csv

docker-clean:
	@echo "Removing Docker image..."
	docker rmi tax-pipeline 2>/dev/null || true
	@echo "Docker cleanup complete!"

# Format code with black and isort
lint:
	@echo "Formatting code with black and isort..."
	cd $(PIPELINE_DIR) && python3 -m black src/ tests/
	cd $(PIPELINE_DIR) && python3 -m isort --profile black src/ tests/

# Check code formatting
check-format:
	@echo "Checking code formatting..."
	cd $(PIPELINE_DIR) && python3 -m black --check --diff src/ tests/

# Check import sorting
check-imports:
	@echo "Checking import sorting..."
	cd $(PIPELINE_DIR) && python3 -m isort --profile black --check-only --diff src/ tests/

# Full clean (data + Docker)
clean-all: clean docker-clean
	@echo "Full cleanup complete!"

