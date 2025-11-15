# Section 2: Data Quality Monitoring

## Background
You have been deployed to a government agency to monitor their data quality. The agency already has existing data pipelines from various data sources.

## Business Objectives
The agency's management wants to know the state of data quality they receive from their sources in an easy to read manner that are mapped to key data quality dimensions that they want to oversee. The data quality dimensions are Completeness, Uniqueness, Validity, Conformity, Consistency, Timeliness.

## Requirements
You have been assigned to monitor the data quality of the dataset 'grant_applications.csv' data provided from another agency. 

### Dataset Schema
The dataset `grant_applications.csv` contains the following fields:

| Field Name | Description | Data Type |
| :--- | :--- | :--- |
| `application_id` | A unique identifier for each grant application. | UUID / String |
| `citizen_nric` | The masked National Registration Identity Card number of the applicant. | String |
| `grant_scheme_name` | The name of the grant scheme being applied for (e.g., "Education Bursary", "Healthcare Subsidy"). | String |
| `household_income` | The declared monthly income of the applicant's household. | Decimal(10, 2) |
| `household_size` | The number of members residing in the same household as the applicant. | Integer |
| `application_date` | The date the application was submitted by the citizen. | Date |
| `application_status` | The current status of the application (e.g., "Pending", "Approved", "Rejected"). | String |
| `requested_amount` | The grant amount requested by the applicant. | Decimal(10, 2) |
| `approved_amount` | The final grant amount approved by the agency. Can be NULL if pending/rejected. | Decimal(10, 2) |
| `decision_date` | The date the final decision was made on the application. | Date |

### Data Quality Rules
Business users have identified some initial data quality rules they want checked:

| Data Quality Rule | Data Quality Dimension |
|----------|----------|
| 'citizen_nric' must follow the format of a letter, 7 digits, and a final letter (e.g., S1234567A) | Validity |
| 'decision_date' must follow the date format of YYYY-MM-DD | Conformity |
| 'application_status' contains only the allowed values ("Pending", "Approved", "Rejected").| Conformity |

However, there are other issues with the data. Identify what additional data quality rules should be set up and what data quality dimension they should be mapped to.

---

## Assignment Tasks

### Task 1: 
**Focus: Data Quality Metrics**

Develop a script in the language of your choice that analyses the data provided in 'Section2/test_data/grant_applications.csv'
and generates data quality metrics which can be aggregated into data quality dimension scores.

#### Score Calculation
For each dimension, the score should be calculated as the percentage of rows that pass **all** rules associated with that dimension.
`Score = (Number of Passing Rows / Total Number of Rows) * 100`

#### Technical Requirements
- Must have:
    - Identify and map additional data quality rules to dimensions
    - Code that analyses data and returns a data quality dimension score for each of the six dimensions.

- Nice to have:
    - Implement a configurable pipeline using YAML/JSON configuration files where rules can be defined.
    - Add comprehensive logging and error handling.

**Deliverables:**
- A data model diagram (e.g., a conceptual schema) illustrating how you would store and organize the data quality results for tracking over time.
- A mapping table of all data quality rules you implemented (both given and proposed) to their corresponding data quality dimensions, with a justification for each mapping.
- Code that produces a dataframe that minimally contains the following fields: (you are advised to propose additional fields to enrich the metadata)
    1. `dimension_name` - name of the data quality dimension
    2. `score` - the % scored for the dimension, rounded to 2 decimal places
