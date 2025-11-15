# Section 1: Data Pipelines & Infrastructure

## Background

A government agency needs to modernize their data analytics platform to support policy making, compliance monitoring, and revenue forecasting. As a Senior Data Engineer, you are tasked with building a robust, scalable data processing pipeline that can handle tax data while ensuring data quality, security, and analytical capabilities.

## Dataset

You have been provided with a sample dataset of 149 individual tax returns from assessment year 2023 (`individual_tax_returns.csv`). This dataset contains realistic Singapore tax data including:

- Taxpayer information (NRIC, demographics)
- Income and tax calculations
- Filing details and compliance data
- Geographic and occupation classifications

**Note**: This dataset intentionally contains data quality issues that mirror real-world scenarios, including missing values, invalid dates, malformed IDs, and calculation inconsistencies. This is the sample data only, the data to validate the solution could reach to 100K records.

## Business Objectives

The agency leadership team needs answers to critical questions such as:
- How has tax collection trended over time?
- What are the income and tax patterns across different demographics?
- Which geographic areas contribute most to tax revenue?
- How do different occupations compare in terms of tax compliance?

## Requirements

Build a comprehensive data engineering solution that transforms raw tax filing data into a production-ready analytics platform supporting government decision-making and compliance monitoring. You have freedom to make your own assumptions but should explain them in the submission's document.

---

## Assignment Tasks

### Task 1: Production Data Pipeline Development
**Focus: Scalable & Maintainable ETL Pipeline**

Develop a robust data processing pipeline that:

#### Data Ingestion & Validation
- Extract data from the provided CSV file
- Implement comprehensive data validation rules based on Singapore tax regulations:
  - Basic NRIC format validation (`[STFG]xxxxxxx[A-Z]`) - there should be a more comprehensive algorithm to validate the NRIC, but it's not required in this assignment
  - Singapore postal code validation (6-digit format)
  - Filing date logic (must be after assessment year)
  - Tax calculation verification (chargeable income = annual income - reliefs)
  - Business rule validation (CPF contributions only for residents)

#### Data Transformation & Cleaning
- Clean and standardize data (handle missing NRICs, invalid dates, malformed postal codes)
- Create data quality scores and flags
- Implement the transformation logic, you can choose a framework of your choices.
- Build dimensional model with proper fact and dimension tables:
  - `dim_taxpayer` (information about tax payer)
  - `dim_time` (date hierarchy with tax-specific attributes)
  - `dim_location` (Singapore geography and housing types)
  - `dim_occupation` (professional categorization)
  - `fact_tax_returns` (transaction-level tax data)

#### Technical Requirements
- Must have:
  - Use Python with object-oriented design patterns (or languages of your choice)
  - Create modular, testable code with proper separation of concerns
  - Handle edge cases and data quality issues gracefully

- Nice to have:
  - Implement configurable pipeline using YAML/JSON configuration files
  - Add comprehensive logging and error handling
  - Containerization using Docker

**Deliverables:**
- ETL pipeline code with modular architecture
- Unit tests for critical validation functions

### Task 2: Cloud Deployment & Infrastructure
Deploy the solution using cloud services with good security practices.

#### Cloud Infrastructure
- Design and implement using AWS/Azure services: provide technical architecture diagram with reason/explanation why you choose certain Cloud service.

**Deliverables:**
- Must have:
  - Cloud architecture diagram with security boundaries.

- Nice to have:
  - Infrastructure as Code scripts with deployment instructions
  - CI/CD pipeline configuration
  - Cost analysis and optimization recommendations

### Task 3: Documentation

**Deliverables:**
- Must have:
  - System overview, data flow diagrams, technology choices

- Nice to have:
  - Complete schema documentation with business rules
  - Step-by-step cloud deployment instructions

## Submission Requirements

1. **Source Code**: A public github repo with complete tested pipeline implementation and required documentation in README file.
2. **Infrastructure**: Cloud deployment scripts and configurations if any
3. **Documentation**: Technical and business documentation package if any

**Estimated Time**: 8-12 hours

