# Data Model

## Overview

This document describes the conceptual data model for storing and tracking data quality monitoring results over time.

## Entity Relationship Diagram (ERD)

```
┌─────────────────────────────────────┐
│         dq_execution                │
│  ─────────────────────────────      │
│  execution_id (PK)                  │
│  execution_timestamp                │
│  dataset_name                       │
│  dataset_version                    │
│  total_rows                         │
│  status (SUCCESS/FAILED)            │
└──────────────┬──────────────────────┘
               │
               │ 1:N
               ▼
┌─────────────────────────────────────┐
│     dq_dimension_scores             │
│  ─────────────────────────────      │
│  execution_id (FK, PK)              │
│  dimension_name (PK)               │
│  score                              │
│  total_rows                         │
│  passing_rows                       │
│  failing_rows                       │
│  rule_count                         │
└──────────────┬──────────────────────┘
               │
               │ 1:N
               ▼
┌─────────────────────────────────────┐
│     dq_rule_results                 │
│  ─────────────────────────────      │
│  execution_id (FK, PK)              │
│  rule_id (PK)                       │
│  dimension_name                     │
│  rule_name                          │
│  total_rows                         │
│  passing_rows                       │
│  failing_rows                       │
│  failure_rate                       │
└─────────────────────────────────────┘
```

## Table Definitions

### dq_execution

**Purpose**: Track each execution of the data quality monitoring pipeline.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `execution_id` | VARCHAR/UUID | Unique identifier for execution | PRIMARY KEY |
| `execution_timestamp` | TIMESTAMP | When the analysis was run | NOT NULL |
| `dataset_name` | VARCHAR(255) | Name of dataset analyzed | NOT NULL |
| `dataset_version` | VARCHAR(50) | Version/identifier of dataset | NULLABLE |
| `total_rows` | INTEGER | Total number of rows analyzed | NOT NULL |
| `status` | VARCHAR(20) | Execution status: SUCCESS, FAILED | NOT NULL |

### dq_dimension_scores

**Purpose**: Store dimension-level scores for each execution.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `execution_id` | VARCHAR/UUID | Foreign key to dq_execution | FOREIGN KEY, NOT NULL |
| `dimension_name` | VARCHAR(50) | Dimension name (Completeness, Uniqueness, etc.) | PRIMARY KEY, NOT NULL |
| `score` | DECIMAL(5,2) | Dimension score (0-100) | NOT NULL |
| `total_rows` | INTEGER | Total rows analyzed | NOT NULL |
| `passing_rows` | INTEGER | Rows passing all rules in dimension | NOT NULL |
| `failing_rows` | INTEGER | Rows failing at least one rule | NOT NULL |
| `rule_count` | INTEGER | Number of rules in this dimension | NOT NULL |

**Composite Primary Key**: (`execution_id`, `dimension_name`)

### dq_rule_results

**Purpose**: Store detailed rule-level results for drill-down analysis.

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| `execution_id` | VARCHAR/UUID | Foreign key to dq_execution | FOREIGN KEY, NOT NULL |
| `rule_id` | VARCHAR(10) | Rule identifier (e.g., C001, V001) | PRIMARY KEY, NOT NULL |
| `dimension_name` | VARCHAR(50) | Dimension this rule belongs to | NOT NULL |
| `rule_name` | VARCHAR(255) | Human-readable rule name | NOT NULL |
| `total_rows` | INTEGER | Total rows evaluated | NOT NULL |
| `passing_rows` | INTEGER | Rows passing this rule | NOT NULL |
| `failing_rows` | INTEGER | Rows failing this rule | NOT NULL |
| `failure_rate` | DECIMAL(5,2) | Failure rate percentage | NOT NULL |

**Composite Primary Key**: (`execution_id`, `rule_id`)

## Relationships

- **dq_execution** (1) → (N) **dq_dimension_scores**: One execution produces scores for multiple dimensions
- **dq_execution** (1) → (N) **dq_rule_results**: One execution produces results for multiple rules

## Use Cases

1. **Historical Tracking**: Track data quality trends over time by querying `dq_execution` and `dq_dimension_scores`
2. **Drill-Down Analysis**: Identify which specific rules are failing by querying `dq_rule_results`
3. **Reporting**: Generate reports showing dimension scores across multiple executions
4. **Alerting**: Set up alerts when dimension scores drop below thresholds

## Implementation Notes

- The current implementation outputs three separate CSV files matching this normalized schema:
  - `dq_execution/`: One row per execution (execution_id, execution_timestamp, dataset_name, dataset_version, total_rows, status)
  - `dq_dimension_scores/`: Multiple rows per execution, one per dimension (execution_id, dimension_name, score, total_rows, passing_rows, failing_rows, rule_count)
  - `dq_rule_results/`: Multiple rows per execution, one per rule (execution_id, rule_id, dimension_name, rule_name, total_rows, passing_rows, failing_rows, failure_rate)
- A minimal output file `dq_dimension_scores_minimal/` is also generated with only `dimension_name` and `score` columns as required by deliverables
- For production, consider implementing this as database tables (PostgreSQL, MySQL, etc.)
- Add indexes on `execution_timestamp` and `dimension_name` for efficient queries
- Consider partitioning by `execution_timestamp` for large-scale deployments
- The `execution_id` is a UUID generated for each run, linking all three tables together

