# Section 2: Data Quality Monitoring

## Overview

A PySpark-based data quality monitoring system that evaluates grant application data across six dimensions: **Completeness**, **Uniqueness**, **Validity**, **Conformity**, **Consistency**, and **Timeliness**.

## Quick Start

### Prerequisites

- Python 3.9+
- **Java 8 or later** (Java 11 or 17 recommended) - Required for PySpark
- uv

### Install Runtime Dependencies (includes dev dependencies for testing)

```bash
# Using Make (recommended)
make install
source .venv/bin/activate

# Or manually with uv
uv sync --extra dev
source .venv/bin/activate
```

### Run Analysis

```bash
# Using Make
make run

# Or directly
python scripts/run_dq_analysis.py --data test_data/grant_applications.csv --output output
```

## Makefile Commands
- `make help`: Show available commands
- `make install`: Install dependencies using uv (includes dev dependencies)
- `make run`: Run data quality analysis
- `make output`: Show latest results
- `make test`: Run unit tests
- `make test-coverage`: Run unit tests with coverage report
- `make clean`: Clean output files and cache


## Project Structure

```
Section2/
├── .github/workflows/      # GitHub Actions workflow
├── docs/                    # Documentations
├── src/
│   ├── config/             # Configuration files
│   ├── rules/              # Data quality rules (25 rules)
│   ├── utils/              # Utilities
│   ├── pipeline/           # Pipeline orchestrator
│   ├── evaluator.py        # Rule evaluator
│   └── dimension_aggregator.py  # Dimension score calculator
├── scripts/
│   └── run_dq_analysis.py  # Main entry point
├── test_data/              # Input data
├── output/                 # Output directory
├── Makefile               # Build automation
└── pyproject.toml         # Project configuration
```

## Data Quality Rules

The system implements **25 rules** across **6 dimensions**:

| Dimension | Rules | Rule IDs |
|-----------|-------|----------|
| Completeness | 6 | C001-C006 |
| Uniqueness | 2 | U001-U002 |
| Validity | 7 | V001-V007 |
| Conformity | 4 | CF001-CF004 |
| Consistency | 4 | CS001-CS004 |
| Timeliness | 2 | T001-T002 |

See [docs/RULE_MAPPING_QUICK_REFERENCE.md](/Section2/docs/RULE_MAPPING_QUICK_REFERENCE.md) for complete rule details.

## Output

The pipeline generates data quality results in a normalized structure matching the data model (see [DATA_MODEL.md](DATA_MODEL.md)):

### Normalized Output Files

1. **`output/dq_execution/`**: Execution metadata (one row per run)
   - Columns: `execution_id`, `execution_timestamp`, `dataset_name`, `dataset_version`, `total_rows`, `status`
   - Tracks each execution of the pipeline

2. **`output/dq_dimension_scores/`**: Dimension-level scores (multiple rows per execution)
   - Columns: `execution_id`, `dimension_name`, `score`, `total_rows`, `passing_rows`, `failing_rows`, `rule_count`
   - One row per dimension per execution

3. **`output/dq_rule_results/`**: Rule-level results (multiple rows per execution)
   - Columns: `execution_id`, `rule_id`, `dimension_name`, `rule_name`, `total_rows`, `passing_rows`, `failing_rows`, `failure_rate`
   - One row per rule per execution (25 rules total)

4. **`output/dq_dimension_scores_minimal/`**: Minimal output for deliverables
   - Columns: `dimension_name`, `score`
   - Required format with only dimension name and score

5. **`output/dq_dimension_scores_full/`**: Legacy format (backward compatibility)
   - Flattened format with all fields in one table

### Example Output

**Minimal Output** (`dq_dimension_scores_minimal/`):
```csv
dimension_name,score
Completeness,93.55
Uniqueness,100.0
Validity,23.75
Conformity,76.24
Consistency,46.37
Timeliness,65.37
```

**Normalized Dimension Scores** (`dq_dimension_scores/`):
```csv
execution_id,dimension_name,score,total_rows,passing_rows,failing_rows,rule_count
bd349fd6-1480-43fb-a804-e03dbd86cb49,Completeness,93.55,10000,9355,645,6
bd349fd6-1480-43fb-a804-e03dbd86cb49,Uniqueness,100.0,10000,10000,0,2
...
```

All tables are linked via `execution_id` for historical tracking and drill-down analysis.


## Configuration

Edit `src/config/rules_config.yaml` to adjust:

- Timeliness thresholds (SLA days, max age years)
- Dataset name
- Logging level
- Spark configuration

## GitHub Actions

The workflow is configured to run:
- **Scheduled**: Daily at 2 AM UTC
- **Manual**: Can be triggered manually via GitHub Actions UI

## Documentation

- **[DATA_MODEL.md](DATA_MODEL.md)**: Data model diagram and schema for storing DQ results over time
- **[RULE_MAPPING_QUICK_REFERENCE.md](RULE_MAPPING_QUICK_REFERENCE.md)**: Complete rule mapping table with justifications


## Troubleshooting

### Java Not Found

```bash
# Check Java installation
java -version

# If not installed, see INSTALL_JAVA.md
# Or install via Homebrew:
brew install openjdk@17
export JAVA_HOME="/opt/homebrew/opt/openjdk@17"
```

### Dependencies Issues

```bash
# Clear cache and reinstall
uv cache clean
uv sync --reinstall
```

### Module Not Found

```bash
# Ensure dev dependencies are installed
uv sync --extra dev

# Ensure virtual environment is activated
source .venv/bin/activate

# Verify installation
python -c "import pyspark; print('OK')"
python -c "import pytest; print('OK')"
```

### Test Failures

If tests fail with missing columns or date parsing errors:

- **Missing columns**: Ensure test data includes all required columns:
  - `grant_scheme_name`, `citizen_nric`, `household_size`, `household_income`
- **Date parsing errors**: The date parser automatically handles both string dates and datetime objects inferred by Spark from CSV files