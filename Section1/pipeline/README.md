# Tax Data Pipeline

Production-ready ETL pipeline for processing tax filing data using Medallion Architecture (Bronze → Silver → Gold) and Star Schema dimensional modeling.

## Architecture

- **Bronze Layer**: Raw CSV ingestion with metadata
- **Silver Layer**: Data validation, cleaning, and quality scoring
- **Gold Layer**: Star schema dimensional model (fact + dimension tables)

For detailed architecture documentation, see [ARCHITECTURE_DESIGN.md](../docs/ARCHITECTURE_DESIGN.md).

## Project Structure

```
pipeline/
├── config/
│   ├── pipeline_config.yaml      # Pipeline configuration
│   └── validation_rules.yaml      # Validation rules
├── src/
│   ├── bronze/                     # Bronze layer
│   │   └── csv_ingester.py
│   ├── silver/                     # Silver layer
│   │   ├── validators.py
│   │   ├── business_rules.py
│   │   ├── cleaner.py
│   │   └── quality_scorer.py
│   ├── gold/                       # Gold layer
│   │   └── dimensional_modeler.py
│   ├── orchestration/
│   │   └── pipeline_orchestrator.py
│   └── utils/
│       ├── logger.py
│       └── spark_session.py
├── tests/
│   └── unit/
│       ├── test_bronze/
│       ├── test_silver/
│       └── test_gold/
├── main.py                         # Entry point
├── requirements.txt
├── Dockerfile
└── README.md
```

## Local Setup

### Prerequisites

- Python 3.10+
- Java 11+ (for Spark)
- pip
- make (optional, for convenience commands)

### Installation

**Using make:**
> run all `make` commands under `Sction1` folder
```bash
make install
```

**Or manually:**
```bash
cd pipeline
pip install -r requirements.txt
```

### Running the Pipeline

**Using make:**
```bash
make run
```

**Or manually:**
```bash
cd pipeline
python main.py --csv-path ../test_data/individual_tax_returns.csv
```

Config files are auto-discovered from standard locations.

### Available Make Commands

```bash
make help          # Show all available commands
make install       # Install dependencies
make run           # Run the pipeline
make test          # Run unit tests
make test-coverage # Run tests with coverage report
make clean         # Clean generated data files
make docker-build  # Build Docker image
make docker-run    # Run pipeline in Docker
```

### Output

Pipeline creates the following directories in `test_output/` (at project root):
- `test_output/bronze/` - Raw data with metadata
- `test_output/silver/` - Cleaned and validated data
- `test_output/gold/` - Star schema tables
- `test_output/rejected/` - Rejected records (DQ < 0.6)

## Docker

**Using make:**
```bash
make docker-build  # Build image
make docker-run    # Run container
```

**Or manually:**
```bash
cd pipeline
docker build -t tax-pipeline .
docker run -v $(pwd)/../test_data:/data tax-pipeline \
  --csv-path /data/individual_tax_returns.csv
```

## Testing

**Using make:**
```bash
make test          # Run tests
make test-coverage # Run tests with coverage
```

**Or manually:**
```bash
cd pipeline
pytest tests/unit/
```

## Configuration

### Pipeline Config (`config/pipeline_config.yaml`)

- Required columns for ingestion
- Storage paths (local/cloud)
- Occupation category mappings

### Validation Rules (`config/validation_rules.yaml`)

- NRIC format validation
- Postal code validation
- Tax calculation tolerance
- CPF business rules
- Data quality weights and thresholds

## Data Quality Scoring

Records are classified based on weighted DQ scores:
- **Accept** (≥ 0.8): Included in Gold layer
- **Quarantine** (0.6 - 0.8): Included in Gold with flag
- **Reject** (< 0.6): Excluded from Gold, stored separately

See [ARCHITECTURE_DESIGN.md](../docs/ARCHITECTURE_DESIGN.md#data-quality-scoring) for the detailed scoring formula.

## Star Schema

The pipeline creates a star schema with dimension and fact tables. See [ARCHITECTURE_DESIGN.md](../docs/ARCHITECTURE_DESIGN.md#star-schema) for complete schema details.

## Cloud Deployment

See [ARCHITECTURE_DESIGN.md](../docs/ARCHITECTURE_DESIGN.md#aws-cloud-architecture-for-tax-data-pipeline) for AWS deployment instructions.
