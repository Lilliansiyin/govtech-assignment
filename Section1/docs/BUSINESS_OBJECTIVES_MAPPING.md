# Business Objectives SQL

This document maps each business objective to its technical implementation using the star schema.

**Related Documentation:**
- **Pipeline Architecture**: See `ARCHITECTURE_DESIGN.md`
- **AWS Cloud Architecture**: See `CLOUD_ARCHITECTURE.md`

---

## Business Objective 1: Tax Collection Trends Over Time

**Question**: "How has tax collection trended over time?"

**Schema Support:**
- `dim_time`: Time hierarchy (year, quarter, month, filing_date, tax_season_flag)
- `fact_tax_returns.tax_paid_sgd`: Actual collection
- `fact_tax_returns.tax_payable_sgd`: Expected collection

**Query Example:**
```sql
-- Monthly tax collection trend
SELECT 
    t.filing_year,
    t.filing_month,
    SUM(f.tax_paid_sgd) as total_collected,
    SUM(f.tax_payable_sgd) as total_expected,
    COUNT(*) as return_count,
    AVG(f.tax_paid_sgd) as avg_collection_per_return
FROM fact_tax_returns f
JOIN dim_time t ON f.time_key = t.time_key
GROUP BY t.filing_year, t.filing_month
ORDER BY t.filing_year, t.filing_month;
```
![](/Section1/docs/imgs/athena-1.png)
---

## Business Objective 2: Income & Tax Patterns by Demographics

**Question**: "What are the income and tax patterns across different demographics?"

**Schema Support:**
- `dim_taxpayer`: Demographics (filing_status, residential_status, number_of_dependents)
- `fact_tax_returns`: Income/tax measures with pre-calculated `effective_tax_rate`

**Query Examples:**

**By Filing Status:**
```sql
SELECT 
    p.filing_status,
    COUNT(*) as taxpayer_count,
    AVG(f.annual_income_sgd) as avg_income,
    AVG(f.tax_payable_sgd) as avg_tax,
    AVG(f.effective_tax_rate) as avg_tax_rate
FROM fact_tax_returns f
JOIN dim_taxpayer p ON f.taxpayer_key = p.taxpayer_key
GROUP BY p.filing_status;
```
![](/Section1/docs/imgs/athena-2.1.png)

**By Residential Status & Dependents:**
```sql
SELECT 
    p.residential_status,
    p.number_of_dependents,
    AVG(f.annual_income_sgd) as avg_income,
    AVG(f.total_reliefs_sgd) as avg_reliefs,
    AVG(f.effective_tax_rate) as avg_tax_rate
FROM fact_tax_returns f
JOIN dim_taxpayer p ON f.taxpayer_key = p.taxpayer_key
GROUP BY p.residential_status, p.number_of_dependents
ORDER BY p.residential_status, p.number_of_dependents;
```
![](/Section1/docs/imgs/athena-2.2.png)
---

## Business Objective 3: Geographic Revenue Contribution

**Question**: "Which geographic areas contribute most to tax revenue?"

**Schema Support:**
- `dim_location`: Region, planning_area (from postal code), housing_type
- `fact_tax_returns.tax_paid_sgd`: Revenue measure

**Query Examples:**

**Revenue by Region:**
```sql
SELECT 
    l.region,
    SUM(f.tax_paid_sgd) as total_revenue,
    COUNT(*) as taxpayer_count,
    AVG(f.tax_paid_sgd) as avg_revenue_per_taxpayer,
    SUM(f.tax_paid_sgd) * 100.0 / 
        (SELECT SUM(tax_paid_sgd) FROM fact_tax_returns) 
        as revenue_percentage
FROM fact_tax_returns f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.region
ORDER BY total_revenue DESC;
```
![](/Section1/docs/imgs/athena-3.1.png)

**Revenue by Housing Type:**
```sql
SELECT 
    l.housing_type,
    SUM(f.tax_paid_sgd) as total_revenue,
    COUNT(*) as taxpayer_count,
    AVG(f.annual_income_sgd) as avg_income
FROM fact_tax_returns f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.housing_type
ORDER BY total_revenue DESC;
```
![](/Section1/docs/imgs/athena-3.2.png)

**Top Contributing Postal Codes:**
```sql
SELECT 
    l.postal_code,
    l.region,
    l.housing_type,
    SUM(f.tax_paid_sgd) as total_revenue,
    COUNT(*) as taxpayer_count
FROM fact_tax_returns f
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY l.postal_code, l.region, l.housing_type
ORDER BY total_revenue DESC
LIMIT 20;
```
![](/Section1/docs/imgs/athena-3.3.png)
---

## Business Objective 4: Occupation Compliance Comparison

**Question**: "How do different occupations compare in terms of tax compliance?"

**Schema Support:**
- `dim_occupation`: Occupation category (Engineering, Management, Healthcare, Finance, etc.)
- `fact_tax_returns.tax_compliance_rate`: Pre-calculated (tax_paid / tax_payable)
- `fact_tax_returns.tax_liability`: Outstanding tax

**Query Examples:**

**Compliance by Occupation Category:**
```sql
SELECT 
    o.occupation_category,
    COUNT(*) as total_returns,
    AVG(f.tax_compliance_rate) as avg_compliance_rate,
    SUM(CASE WHEN f.tax_compliance_rate >= 0.95 THEN 1 ELSE 0 END) 
        as fully_compliant_count,
    SUM(CASE WHEN f.tax_compliance_rate < 0.95 THEN 1 ELSE 0 END) 
        as non_compliant_count,
    SUM(f.tax_liability) as total_outstanding,
    AVG(f.annual_income_sgd) as avg_income
FROM fact_tax_returns f
JOIN dim_occupation o ON f.occupation_key = o.occupation_key
GROUP BY o.occupation_category
ORDER BY avg_compliance_rate DESC;
```
![](/Section1/docs/imgs/athena-4.1.png)

**Detailed Compliance by Specific Occupation:**
```sql
SELECT 
    o.occupation,
    o.occupation_category,
    COUNT(*) as return_count,
    AVG(f.tax_compliance_rate) as avg_compliance,
    AVG(f.annual_income_sgd) as avg_income,
    SUM(f.tax_liability) as total_liability
FROM fact_tax_returns f
JOIN dim_occupation o ON f.occupation_key = o.occupation_key
GROUP BY o.occupation, o.occupation_category
HAVING COUNT(*) >= 5  -- Filter for meaningful sample size
ORDER BY avg_compliance DESC;
```
![](/Section1/docs/imgs/athena-4.2.png)

---

## Cross-Objective Analytics

**Example: Compliance by Region and Filing Status**
```sql
SELECT 
    l.region, p.filing_status,
    AVG(f.tax_compliance_rate) as avg_compliance,
    COUNT(*) as taxpayer_count
FROM fact_tax_returns f
JOIN dim_location l ON f.location_key = l.location_key
JOIN dim_taxpayer p ON f.taxpayer_key = p.taxpayer_key
GROUP BY l.region, p.filing_status;
```

---

## Why Star Schema Works

1. **Flexible Aggregation**: Easy grouping by any dimension combination
2. **Performance**: Pre-calculated measures reduce computation
3. **Scalability**: Partitioning enables efficient large dataset queries
4. **Maintainability**: Clear separation of dimensions and facts
