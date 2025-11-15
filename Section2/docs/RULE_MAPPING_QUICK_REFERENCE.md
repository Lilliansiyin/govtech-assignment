# Data Quality Rules Mapping Table

This document provides a complete mapping of all data quality rules to their corresponding dimensions with justifications.

## Rule Count by Dimension

| Dimension | Rule Count | Rule IDs |
|-----------|------------|----------|
| Completeness | 6 | C001-C006 |
| Uniqueness | 2 | U001-U002 |
| Validity | 7 | V001-V007 |
| Conformity | 4 | CF001-CF004 |
| Consistency | 4 | CS001-CS004 |
| Timeliness | 2 | T001-T002 |
| **TOTAL** | **25** | |

---

## Details

### Completeness (6 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| C001 | Required field present | `application_id` | Not null/empty | Primary key; missing prevents record identification |
| C002 | Required field present | `application_date` | Not null/empty | Required for temporal analysis and SLA tracking |
| C003 | Required field present | `application_status` | Not null/empty | Required for workflow state management |
| C004 | Required field present | `requested_amount` | Not null | Required for financial processing |
| C005 | Conditional required | `approved_amount` | Not null when status = "Approved" | Approved applications must have approved value for reporting |
| C006 | Conditional required | `decision_date` | Not null when status in ("Approved", "Rejected") | Decision date mandatory when decision exists |

### Uniqueness (2 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| U001 | Primary key unique | `application_id` | No duplicates | Primary key constraint; duplicates indicate ingestion/deduplication issues |
| U002 | No duplicate rows | All columns | No exact duplicates | Repeated identical rows may indicate delivery retry or data duplication |

### Validity (7 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| V001 | NRIC format | `citizen_nric` | Regex: `^[STFG]\d{7}[A-Z]$` (also checks not null) | Field must be valid NRIC pattern; sample contains invalids (INVALID_NRIC, digits-only) |
| V002 | Income range | `household_income` | >= 0 and < 1e7 | Negative incomes invalid; cap prevents obviously wrong values |
| V003 | Amount range | `requested_amount` | >= 0 | Negative amounts are invalid |
| V004 | Amount range | `approved_amount` | >= 0 (when not null) | Negative approved amounts invalid |
| V005 | Household size range | `household_size` | Integer, 1-20 | Invalid household counts (0, negatives, non-int) are data errors |
| V006 | Business rule | `approved_amount` | Only exists when status = "Approved" | Business rule: approved amount should only exist for approved applications |
| V007 | No placeholders | Multiple | No "NOT_A_DATE", "INVALID_NRIC", etc. | Placeholder values indicate data quality issues |

### Conformity (4 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| CF001 | Date format ISO | `decision_date` | YYYY-MM-DD format (must be parseable as valid date) | Standardized date format required for system integration |
| CF002 | Date format ISO | `application_date` | YYYY-MM-DD or canonicalizable to ISO (must be parseable as valid date) | Dates in inconsistent formats need standardization |
| CF003 | Status canonical | `application_status` | Exact match: "Pending", "Approved", "Rejected" | Raw value must belong to accepted set after normalization |
| CF004 | Scheme name catalog | `grant_scheme_name` | In ("Education Bursary", "Healthcare Subsidy", "Skills Upgrading Grant") | Enable consistent reporting and grouping |

### Consistency (4 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| CS001 | Temporal logic | `decision_date`, `application_date` | decision_date >= application_date | Decision cannot occur before application submission |
| CS002 | Amount logic | `approved_amount`, `requested_amount` | approved <= requested when Approved | Approved amount should not exceed requested; must be positive |
| CS003 | Status-date logic | `application_status`, `decision_date` | decision_date NULL when Pending | Having decision_date but status Pending is inconsistent |
| CS004 | Status-date logic | `application_status`, `decision_date` | decision_date NOT NULL when Approved/Rejected | Decision date required when decision made |

### Timeliness (2 rules)

| ID | Rule | Field(s) | Check | Justification |
|----|------|----------|-------|---------------|
| T001 | SLA compliance | `decision_date`, `application_date` | decision_lag <= 30 days | SLA monitoring; rows exceeding SLA flagged as late |
| T002 | Data freshness | `application_date` | Not > 5 years old | Source might be delayed; old dates indicate late delivery |

---

## Given Rules (from README)

| Rule | Dimension | Our Mapping |
|------|-----------|-------------|
| 'citizen_nric' format: letter + 7 digits + letter | Validity | **V001** (also checks not null) |
| 'decision_date' format: YYYY-MM-DD | Conformity | **CF001** (must be parseable as valid date) |
| 'application_status' in ("Pending", "Approved", "Rejected") | Conformity | **CF003** (exact match) |

---

## Score Calculation

For each dimension:
- A row **passes** the dimension if it passes **ALL** rules in that dimension
- Score = (Number of rows passing ALL rules / Total rows) × 100

Example:
- Completeness has 6 rules (C001-C006)
- Row passes Completeness only if it passes C001 AND C002 AND ... AND C006
- Completeness Score = (rows passing all 6 rules / total rows) × 100

