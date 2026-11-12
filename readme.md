# Job Market ELT Pipeline: Ingestion, Profiling & Data Quality

## 📖 Overview
This document records the critical data engineering concepts, ingestion strategies, and architectural decisions applied in this project. It highlights the solutions to common data quality pitfalls, from raw Parquet ingestion to Staging layer deduplication and semi-structured data cleaning.

---

## 1. Raw Ingestion Strategy (Source Layer)

### 1.1 Automated File Download to Volume
**Solution**: Use Python `urllib` to stream-download 12 months of `.parquet` files directly into a Volume `source` directory.

```python
import urllib.request, os
VOLUME = "/Volumes/dbt_luke/raw/source/job_postings"
FILES = [
    "raw_job_postings_2025-07.parquet", "raw_job_postings_2025-08.parquet",
    "raw_job_postings_2025-09.parquet", "raw_job_postings_2025-10.parquet",
    "raw_job_postings_2025-11.parquet", "raw_job_postings_2025-12.parquet",
    "raw_job_postings_2026-01.parquet", "raw_job_postings_2026-02.parquet",
    "raw_job_postings_2026-03.parquet", "raw_job_postings_2026-04.parquet",
    "raw_job_postings_2026-05.parquet", "raw_job_postings_2026-06.parquet"
]
REPO = "lukebarousse/dbt_Analytics_Engineering_Course"
TAG = "dataset-v1"

for name in FILES:
    dest = f"{VOLUME}/{name}"
    if os.path.exists(dest): continue
    url = f"https://github.com/{REPO}/releases/download/{TAG}/{name}"
    urllib.request.urlretrieve(url, dest)
```

### 1.2 Internal Storage Registration
Register Parquet files into the `raw` schema:

```sql
-- Register raw_job_skills table
CREATE OR REPLACE TABLE dbt_luke.raw.raw_job_skills AS
SELECT * FROM parquet.`/Volumes/dbt_luke/raw/source/raw_job_skills.parquet`;

-- Register jobs_per_month table
CREATE OR REPLACE TABLE dbt_luke.raw.jobs_per_month AS
SELECT * FROM parquet.`/Volumes/dbt_luke/raw/source/jobs_per_month.parquet`;

-- Register monthly_summary table
CREATE OR REPLACE TABLE dbt_luke.raw.monthly_summary AS
SELECT * FROM parquet.`/Volumes/dbt_luke/raw/source/monthly_summary.parquet`;

-- Register top_locations table
CREATE OR REPLACE TABLE dbt_luke.raw.top_locations AS
SELECT * FROM parquet.`/Volumes/dbt_luke/raw/source/top_locations.parquet`;
```

### 1.3 Batch Preview with Python Loop
Avoid Databricks UI overwriting `display()`:

```python
tables = ["job_postings", "jobs_per_month", "monthly_summary", "raw_job_skills", "top_locations"]

for t in tables:
    print(f"================ {t.upper()} ================")
    display(spark.read.table(f"dbt_luke.raw.{t}").limit(5))
```

### 1.4 Reference Data via `dbt seed`
`skill_categories` loaded via `dbt seed` from local CSV.

---

## 2. Data Profiling & Exploration

### 2.1 Search Term Distribution
*(Corresponds to video at 04:38:00)*
```sql
SELECT search_term, COUNT(*) AS total
FROM dbt_luke.source.job_postings
WHERE error IS NOT TRUE
GROUP BY 1 ORDER BY total DESC;
```
**Results**: Data Engineer (413,320), Data Analyst (176,350), Data Scientist (102,280).

### 2.2 Boolean Field Profiling (Crucial Discovery)
```sql
SELECT DISTINCT job_work_from_home 
FROM dbt_luke.source.job_postings 
WHERE error IS NOT TRUE;
```
**Result**: Natively **Boolean**, only `true` and `null` (no `false`).

---

## 3. Deep Dive: stg_job_postings (dbt Bronze / Staging)

### 3.1 Text Prefix Cleaning
- **Profiling**: `WHERE job_via LIKE 'via %'`.
- **Cleaning**: `REGEXP_REPLACE(job_via, '^via ', '')`.

### 3.2 Defensive Boolean Casting
```sql
case
    when lower(trim(cast(job_work_from_home as string))) = 'true' then true
    when lower(trim(cast(job_work_from_home as string))) = 'false' then false
    else false
end as job_work_from_home
```

### 3.3 Complex Salary Text Extraction
- **Period**: `regexp_extract` for `year/hour`, `nullif` for empties.
- **Currency**: Extracts leading non-numeric chars, defaults to `USD`.
- **Macro (`parse_salary`)**: Split delimiters, recognize `K`/`M`, `TRY_CAST`.

### 3.4 JSON Array Parsing with Custom Macro
```sql
{% macro unpack_extensions(array_col, keywords) %}
    {% for kw in keywords %}
        coalesce(array_contains({{ array_col }}, '{{ kw }}'), false) as has_{{ kw | replace(' ', '_') | lower }}
        {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
{% endmacro %}
```
One-line call:
```sql
{{ unpack_extensions('job_extensions', ['Health insurance', 'Dental insurance', 'Paid time off', 'No degree mentioned']) }}
```

### 3.5 Deduplication
```sql
row_number() over (partition by job_id order by search_date desc) as row_num
```

### 3.6 Reconciliation: Data Funnel
```sql
SELECT 
    COUNT(*) AS total_rows_raw,
    COUNT(CASE WHEN error IS NOT TRUE THEN 1 END) AS total_rows_valid1,
    COUNT(CASE WHEN error IS NOT TRUE AND job_id IS NOT NULL THEN 1 END) AS total_rows_valid,
    (SELECT COUNT(*) FROM dbt_luke.bronze.stg_job_postings) AS total_rows_bronze
FROM dbt_luke.source.job_postings;
```
**Audit Results**:
- Source Total: **843,097**
- Valid after error filter: **691,950**
- Valid after null ID filter: **685,695**
- Final Bronze rows: **491,140**

### 3.7 Bronze Layer Full Table Reconciliation
```sql
SELECT 'job_postings' AS table_name, COUNT(*) AS total_rows FROM dbt_luke.bronze.stg_job_postings
UNION ALL
SELECT 'jobs_per_month', COUNT(*) FROM dbt_luke.bronze.stg_jobs_per_month
UNION ALL
SELECT 'monthly_summary', COUNT(*) FROM dbt_luke.bronze.stg_monthly_summary
UNION ALL
SELECT 'raw_job_skills', COUNT(*) FROM dbt_luke.bronze.stg_job_skills
UNION ALL
SELECT 'top_locations', COUNT(*) FROM dbt_luke.bronze.stg_top_locations
UNION ALL
SELECT 'skill_categories', COUNT(*) FROM dbt_luke.bronze.skill_categories;
```

---

## 4. Data Modeling & Marts Layer (Gold)

After rigorous cleaning in the Staging layer, the `marts` layer (Gold) adopts the classic **Kimball Star Schema**.

### 4.1 Directory Structure
```text
models/
├── staging/          # Bronze: Basic cleaning (stg_*)
├── intermediate/     # Silver: Pre-aggregation to prevent fan-out (int_*)
└── marts/            # Gold: Star Schema, BI-ready outputs
    ├── fct_job_postings.sql
    ├── dim_company.sql
    ├── dim_skill.sql
    └── bridge_job_skills.sql
```

### 4.2 Star Schema Design
- **Fact Table (`fct_job_postings`)**: Records immutable business events, containing foreign keys to dimensions and metrics like salary/benefits.
- **Dimension Tables (`dim_company`, `dim_skill`)**: Store descriptive attributes.
- **Bridge Table (`bridge_job_skills`)**: Resolves the **Many-to-Many** relationship between jobs and skills.

### 4.3 Architecture Review: Why SCD2 on a Fact Table is Wrong
- **Insight**: Fact tables record immutable events. Applying SCD2 here leads to infinite storage growth and logical confusion.
- **Correct Design**: Keep facts as incremental `MERGE` tables. For history, rely on Delta Lake Time Travel.

### 4.4 Architecture Limitation: The `dim_company` Lineage Trap
- **The Limitation**: `dim_company` is derived from `fct_job_postings`. Because they share the same lineage root, an orphaned key is structurally impossible, making the `relationships` test redundant.
- **Best Practice**: In a real enterprise scenario, `dim_company` should be sourced from an independent MDM system.

### 4.5 Bridge Table Many-to-Many & Grain Traps

When building the `bridge_job_skills` (bridging the Fact and Skill Dimension), we encountered a classic Many-to-Many grain conflict:
- **Business Reality**: For the same `job_id` and `skill_id`, text extraction may yield multiple different `skill_keyword`s (e.g., `pub/sub`, `google pub`, `pubsub`). This means the composite of all three columns is unique, but `job_id + skill_id` alone is not.
- **Testing Trap**: Testing uniqueness solely on `(job_id, skill_id)` results in massive test failures (100k+ duplicates), which can mislead developers into blindly deduplicating and losing valuable keyword context.

**Solution & Test Config**:
To balance "retaining keyword details" with "bridge consistency," the true Grain of the Bridge table must be declared as `(job_id, skill_id, skill_keyword)`.
We used `dbt_utils.unique_combination_of_columns` in `_marts_models.yml` to lock this down perfectly:

```yaml
  - name: bridge_job_skills
    description: "Bridge table mapping jobs to skills, retaining keyword variations."
    data_tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - job_id
            - skill_id
            - skill_keyword # Test grain must exactly match the table design
    columns:
      - name: job_id
        data_tests:
          - not_null
          - relationships:
              to: ref('fct_job_postings')
              field: job_id
      - name: skill_id
        data_tests:
          - not_null
          - relationships:
              to: ref('dim_skill')
              field: skill_id
```              

### 4.6 Anti-Pattern: The `dim_company` Lineage Trap & Redundant Test

In the Marts layer tests, the original tutorial runs a `relationships` test from `fct_job_postings.company_name` to `dim_company.company_name`, which successfully passes.
**However, this test is architecturally meaningless (redundant).**

**Root Cause**:
The SQL for `dim_company` is simply extracting `distinct company_name` directly from `fct_job_postings`. They share the exact same lineage. Since the dimension is derived from the fact, an orphaned key is structurally impossible unless transformation logic drops rows. Running this test is like "proving your father is your father."

**Best Practice**:
In a real production environment, `dim_company` should be sourced from an independent Master Data Management (MDM) system or CRM table. Only when the dimension has an independent source can the `relationships` test truly validate referential integrity and prevent the fact table from referencing companies that don't exist in the master data.

### 4.7 Historical Tracking: SCD Type 2 Snapshot Design

While SCD2 is not recommended directly on Fact tables, we need to track historical changes for specific attributes of job postings. We created an SCD2 snapshot based on the deduplicated `stg_job_postings` and stored it in a separate `snapshots` schema:

```sql
{% snapshot scd_job_postings %}
    {{ config(
      target_schema='snapshots',        
      unique_key='job_id',
      strategy='check',
      check_cols=[
        'search_term', 'search_date', 'search_location', 'job_title', 
        'company_name', 'job_location', 'source_platform', 'job_schedule_type', 
        'job_work_from_home', 'job_posted_at', 'job_salary', 'salary_min', 
        'salary_max', 'salary_avg', 'salary_period', 'salary_currency', 
        'has_health_insurance', 'has_dental_insurance', 'has_paid_time_off', 'has_no_degree_mentioned'
      ]
    )}}
    select * from {{ ref('stg_job_postings') }}
{% endsnapshot %}
```

*Note: Under the `check` strategy, dbt ignores `updated_at` and directly compares `check_cols`. It must be based on the `stg` layer (deduplicated with `rn=1`), otherwise it triggers a `Multiple source rows matched` error.*

### 4.8 Practical Verification: How to Prove SCD2 Works

To verify that the snapshot truly captures historical changes, we performed a "data tampering" drill:

1. **Initial State**: The `Senior Data Engineer` role at `Fraser Health` has `job_work_from_home = null`. Run `dbt snapshot` to record version 1.
2. **Simulate Source Data Change**:
   ```sql
   UPDATE dbt_luke.raw.job_postings
   SET job_work_from_home = true
   WHERE job_title = 'Senior Data Engineer' AND company_name = 'Fraser Health';
   ```
3. **Run Pipeline**: Execute `dbt run --select stg_job_postings` and `dbt snapshot`.
4. **Verify Results**: Query the snapshot table, and two rows appear:
   - Old record: `job_work_from_home = null`, `dbt_valid_to` marked as current time (expired).
   - New record: `job_work_from_home = true`, `dbt_valid_to = NULL` (currently active).

### 4.9 SCD2 Tracking Boundary: Raw Ledger vs. Snapshot State Machine

Through practice, we reached a deep architectural conclusion: **Why does a Snapshot only capture the history of the "most recent state"?**

1. **Raw Layer (Raw Ledger)**: It may perform daily overwrites (`UPDATE`) or appends (`APPEND`). If it's an `UPDATE`, the old data is physically destroyed, and history is permanently lost.
2. **Staging Layer (Daily Funnel)**: Using `row_number() = 1`, it masks the complex update mechanisms of the Raw layer, outputting only a **unique "latest slice"** downstream every day.
3. **Snapshot Layer (Historical Polaroid)**: It doesn't process massive ledgers. It focuses solely on the "latest slice" fed by the Staging layer. If any content changes (based on `check_cols`), it takes a photo, seals the old version, and generates a new version.

**Conclusion**: SCD2 does not track all raw scraping logs; instead, it tracks the **"State Machine" history of substantive changes in business attributes**. Without the Snapshot, the Staging layer outputs the latest state daily, and history vanishes upon overwrite; the Snapshot guards this final timeline defense.

---

## 5. Data Quality Testing (Production-Grade)

14 generic tests implemented in `_staging_models.yml`:

```yaml
version: 2

models:
  - name: stg_job_postings
    columns:
      - name: job_id
        data_tests:
          - unique
          - not_null
      
  - name: stg_job_skills
    columns:
      - name: job_id
        data_tests:
          - not_null
          - relationships:
              to: ref('stg_job_postings')
              field: job_id
      - name: skill_id
        data_tests:
          - not_null
          - relationships:
              to: ref('skill_categories')
              field: skill_id

  - name: stg_jobs_per_month
    columns:
      - name: month_year
        data_tests:
          - not_null

  - name: stg_monthly_summary
    columns:
      - name: summary_month
        data_tests:
          - unique
          - not_null
      - name: total_postings
        data_tests:
          - not_null
      - name: remote_postings
        data_tests:
          - not_null
          
  - name: skill_categories
    columns:
      - name: skill_id
        data_tests:
          - unique
          - not_null
      - name: display_name
        data_tests:
          - not_null
```

**Targeted Execution & Results**:
```bash
dbt test --select stg_job_postings stg_job_skills stg_jobs_per_month stg_monthly_summary skill_categories --exclude test_type:singular
```
**Verification Result**: `PASS=14 WARN=0 ERROR=0 SKIP=0 TOTAL=14`.

### 5.1 Singular Test: Salary Anomaly Monitoring

During data profiling, we identified extreme salary values (e.g., annual salaries over $1,000,000). Throwing a hard `ERROR` for these in the pipeline would break the entire build; however, ignoring them would severely skew the average salary calculations in BI dashboards.

Therefore, we created a **Singular Test (`assert_salaries_sane`)** and configured it as a **WARN** level:

- **Test Logic**: Flags records where `salary_period = 'year'`, `salary_currency = 'USD'`, and `salary_max >= 1,000,000`.
- **Business Insight**: Without the USD filter, cross-currency aggregation triggers 7,232 false alarms (currencies like INR ₹, ZAR, and PKR naturally use 7-figure numbers). By pinning it to USD, the anomalies drop to just 21 rows.
- **Root Cause Analysis**: Most of these 21 rows are employer data entry errors (e.g., a role at Kobie Marketing legitimately ranged from 90K–125K, but one scrape mistakenly recorded it as 800K–1M).
- **Handling Strategy**: Set to `severity = 'warn'` instead of `error`. This represents **data reality**, not a broken pipeline. The test results are used to pinpoint dirty data sources precisely without blocking the data flow.

```sql
-- assert_salaries_sane.sql
{{ config(severity = 'warn') }}

SELECT
    job_id,
    company_name,
    job_title,
    salary_min,
    salary_max,
    salary_period,
    salary_currency,
    searched_at
FROM {{ ref('stg_job_postings') }}
WHERE salary_period = 'year'
  AND salary_currency = 'USD'
  AND COALESCE(salary_max, salary_min) >= 1000000
```

---

## 6. Intermediate Layer: Preventing Fan-out
- **Solution**: Use `GROUP BY` in the Intermediate layer to pre-aggregate data before joining dimensions, massively reducing row count and improving performance.

### 6.1 Profiling Query: Deduplication Audit (Checking for Fan-out)

Before moving into the Intermediate layer to build aggregated tables, we must verify that the `stg_job_skills` bridge table does not contain duplicate records for the same `job_id` under the same `skill_id` (which would indicate a Fan-out issue).

The following profiling SQL intuitively compares the "total row count" with the "distinct row count" to pinpoint duplicates:

```sql
select 
    skill_id,
    count(job_id) as total_job_per_skill,          -- Total row count for this skill (including duplicates)
    count(distinct job_id) as unique_job_per_skill,-- Distinct job_id count for this skill
    count(job_id) - count(distinct job_id) as dup_rows, -- The difference is the number of duplicate rows
    case 
        when count(job_id) = count(distinct job_id) then 'OK' 
        else 'DUPLICATED' 
    end as status
from dbt_luke.bronze.stg_job_skills
group by skill_id
order by dup_rows desc, total_job_per_skill desc;
```

**Result & Conclusion**: If `dup_rows` is 0 across the board, it proves that the `(job_id, skill_id)` composite key is unique and no underlying Fan-out has occurred. This provides a guarantee of data quality for the next step: direct `GROUP BY` aggregation in the Intermediate layer.

### 6.2 Intermediate Layer Design: `int_skill_demand` & Fan-out Prevention

To avoid exponential data explosion (Fan-out) caused by Many-to-Many `JOIN`s directly on the Fact table (`fct_job_postings`), we built the `int_skill_demand` model in the Intermediate layer.

**Design Logic**:
1. **Aggregate First, Join Later**: In the Intermediate layer, we use `GROUP BY skill_id` to pre-aggregate millions of detailed rows from `stg_job_skills` into per-skill demand counts (`postings_with_skill`).
2. **Eliminating Fan-out**: Because of this pre-aggregation, when this lightweight `int_skill_demand` table is finally joined with the Fact table (or Dimension tables), **there is only one row per skill**, completely preventing the data inflation issue where "a job with 5 skills becomes 5 rows after a Join".
3. **Performance Boost**: By pushing heavy aggregation operations down to the Intermediate layer, the final BI-ready Marts layer only needs to process lightweight aggregated results, significantly improving query performance.

## 7. Architecture Review: Why SCD2 Snapshot on a Fact Table is Wrong
- **The Issue**: The original tutorial used a dbt `snapshot` (SCD Type 2) on the Fact table to track changes.
- **The Insight**: In Kimball dimensional modeling, **fact tables record immutable events**. They should not have SCD2 applied, as it causes infinite storage growth and logical confusion. SCD2 is strictly for **Dimension tables**. 
- **The Correct Design**: Kept the Fact table as an incremental `MERGE` table (only keeping the latest state), and relied on Databricks' native Delta Lake Time Travel for history tracking if ever needed.

## 8. Architecture Limitation: The `dim_company` Lineage Trap
- **The Issue**: In the final data model, `dim_company` is derived directly from `fct_job_postings` (since company info is embedded in the job post). 
- **The Limitation**: Because `dim_company` shares the exact same lineage/root as the fact table, running a `relationships` test (checking if every fact's `company_id` exists in `dim_company`) is structurally redundant. Since the dimension is derived from the fact, an orphaned key is impossible unless transformation logic drops rows. 
- **Best Practice**: In a real enterprise scenario, `dim_company` should ideally be sourced from an independent Master Data Management (MDM) system or a dedicated CRM table, not inferred from job postings.

## 9. Data Quality Audit: Deviation from Tutorial & Root Cause Analysis

After completing the Marts layer, we reconciled our results with the original tutorial (at 6h14m). Through cross-validation of specific roles and the whole market, we verified accuracy and identified underlying data issues in the original tutorial.

### 9.1 Micro-Level Reconciliation: Data Inflation in Specific Role

When filtering for `search_term = 'Data Engineer'`:

| Skill | Original Tutorial | Our Project | Diff |
| :--- | :--- | :--- | :--- |
| **Python** | 191,777 | **189,970** | + 1,807 |
| **SQL** | 187,451 | **185,900** | + 1,551 |
| **AWS** | 121,066 | **120,279** | + 787 |

**Root Cause**: The tutorial's inflated numbers stem from `job_id` duplication during incremental merges. 
**Our Optimization**: We enforced strict deduplication via `row_number() over (partition by job_id) = 1` in the Fact table. This guarantees absolute uniqueness, making our percentages (e.g., Python at 64.20%) highly accurate.

### 9.2 Macro-Level Reconciliation: Whole Market Validation

Removing the `WHERE` filter to cross-verify the entire job market (Tutorial console only output Top 5):

| Rank | Skill | Original Count | Our Count | Original Pct | Our Pct (2 decimals) |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **SQL** | 270,017 | **270,017** | 55.0% | **54.98%** |
| 2 | **Python** | 269,380 | **269,380** | 54.8% | **54.85%** |
| 3 | **AWS** | 140,433 | **140,433** | 28.6% | **28.59%** |
| 4 | **Azure** | 134,663 | **134,663** | 27.4% | **27.42%** |
| 5 | **Spark** | 116,730 | **116,730** | 23.8% | **23.77%** |

**Deep Dive**: The top 5 absolute demand counts match exactly, proving the macro statistical baseline is correct. The slight percentage variances (e.g., SQL 55.0% vs 54.98%) are due to the tutorial rounding to 1 decimal place and potential minor pollution in the denominator from duplicate merges. Our strict deduplication and 2-decimal rounding ensure absolute precision for macro-level analytics.

### 9.3 Deep Validation: Cleaning "Zombie Data" in Data Analyst Roles

Beyond the macro market, we cross-verified the highly dynamic `search_term = 'Data Analyst'`. This comparison solidifies the historical data accumulation issue in the original tutorial:

| Rank | Skill | Original Count | Our Count | Diff (Redundant) | Original Pct | Our Pct |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | **SQL** | 57,292 | **54,695** | +2,597 | 43.4% | **42.92%** |
| 2 | **Excel** | 41,473 | **40,544** | +929 | 31.4% | **31.81%** |
| 3 | **Python** | 38,611 | **36,089** | +2,522 | 29.3% | **28.32%** |
| 4 | **Power BI** | 36,258 | **35,020** | +1,238 | 27.5% | **27.48%** |
| 5 | **Tableau** | 28,654 | **27,585** | +1,069 | 21.7% | **21.64%** |

**Root Cause**:
Why do the specific DA counts differ by thousands while the macro market matches? The Data Analyst role has extremely high turnover, leading to many expired postings. The original tutorial's incremental model lacked strict boundary cleaning, resulting in **overlapping and duplicated `job_id`s** from past months. 
Our `fct_job_postings` strictly enforces `row_number() over (partition by job_id) = 1` deduplication and restricts incremental predicates to recent data windows, effectively cleaning out these "zombie jobs." Thus, our 54,695 DA count reflects the **most accurate and cleanest demand** on the market.

### 9.4 The Smoking Gun: Primary Key Disaster in the Tutorial's Fact Table

By comparing the `dbt test` execution logs, we found the definitive evidence of all data deviations—the tutorial's `fct_job_postings` table experienced catastrophic failures in both `not_null` and `unique` tests:

- `FAIL 6255 not_null_fct_job_postings_job_id` (**6,255 rows** with null primary keys)
- `FAIL 63207 unique_fct_job_postings_job_id` (**63,207 rows** with duplicate primary keys)

**🧮 Mathematical Proof**:
1. **Our Precise Calculation**:
   - Our Fact table total rows (clean data): **491,140**
   - SQL absolute demand count: 270,017
   - Precise percentage: `270,017 / 491,140 ≈ 54.98%`.
2. **Data Pollution in the Original Tutorial**:
   - Because the tutorial's Fact table contained **63,207** duplicate records and **6,255** null rows, its underlying data was heavily "inflated." This polluted the denominator during calculations, resulting in the inflated `55.0%` ratio.
   - Conversely, our project strictly applies `row_number() over (partition by job_id order by search_date desc) = 1` for deduplication in the `fct_job_postings` incremental model. Combined with `unique_key` and incremental predicates, it **completely eliminates data inflation and primary key conflicts**.

**Core Conclusion**:
While the original tutorial demonstrates the ELT construction process, it has massive vulnerabilities in production-grade data quality. Our project proves that our data pipeline is cleaner, more precise, and more production-ready than the original tutorial through strict primary key deduplication, incremental defenses, and grain-level testing.

### 9.5 Code-Level Comparison: Incremental Architecture & Defensive Design

Comparing our Fact table (`fct_job_postings`) incremental model with the original tutorial reveals a massive gap in production-grade robustness:

1. **Incremental Predicates & Late-arriving Data**:
   - **Tutorial**: Uses a rigid `WHERE search_date > (SELECT MAX(search_date) ...)`. This is fragile, misses late-arriving data, and fails during multiple daily runs.
   - **Our Project**: Double defense. We use `incremental_predicates` in the `config` to physically limit the Merge partition range (only updating the last 2 days), combined with a logical filter `WHERE search_date >= date_sub(..., 2)` in the data flow. This captures late data while preventing full-table scans, massively boosting performance.

2. **Strict Schema Contract**:
   - **Tutorial**: Uses `SELECT *`. Adding upstream columns can trigger Delta Lake Schema errors or data pollution.
   - **Our Project**: Explicitly declares all 22 business fields. This builds a solid data contract, keeping the Fact table structure stable regardless of upstream changes.

3. **Rigor in Deduplication**:
   - **Tutorial**: Relies solely on `ORDER BY searched_at DESC`.
   - **Our Project**: Uses `ORDER BY search_date desc, searched_at desc`, paired with strict `not_null` and `unique` tests, completely eliminating the tens of thousands of duplicate rows and null primary keys seen in the tutorial.

### 9.6 Last-Minute Merge Implementation & Architecture Reflection

In the 8.5-hour tutorial, the original author only added the `incremental_strategy='merge'` configuration to the `fct_job_postings` Fact table in the final 30 minutes.

Prior to that, whether he was using a full rebuild or a blind append, the pressure of full-table scans and data inflation as daily scraped data accumulated was fatal. Introducing `is_incremental()` and Merge in the last 30 minutes was a last-minute patch for this architectural flaw.

**Our Architectural Foresight**:
Facing the same 500k rows of data, our project avoided the full-rebuild detour from day one and established a production-grade incremental defense directly. Crucially, by using `incremental_predicates` to lock the physical Merge scope to the last 2 days, we completely avoided the daily full-table scans and comparisons of hundreds of thousands of rows. This proves that our architecture was benchmarked against the highest industry standards for **Idempotency** and **Performance Optimization** from the very beginning.

## 10. Production Scheduling: Lakeflow Job Orchestration & Execution

To achieve automated ELT processes in the Databricks cloud, we created a multi-task **Lakeflow Job** (Workflow) to chain Data Ingestion and dbt Transformations. The Job is structured into two main tasks with a strict dependency:

### 10.1 Task 1: Ingestion (The EL Step)
- **Task Type**: Notebook / Python
- **Action**: Automatically downloads 12 months of Parquet data files from GitHub releases directly into a Unity Catalog Volume, and registers them as physical tables in the `raw` schema.
- **Safety**: Idempotency is ensured; if files already exist, the script skips downloading and only re-lands the tables.

### 10.2 Task 2: Transformation (dbt Execution)
- **Task Type**: dbt (Source: Git provider)
- **Dependency**: Configured to **run only after Task 1 successfully completes**, ensuring the raw data is ready before transformation.
- **Action**: Executes a precise 7-step dbt command sequence to ensure cloud execution stability and error traceability:

```bash
# 1. Install dependency packages (e.g., dbt_utils)
dbt deps

# 2. Load seed data (e.g., skill_categories)
dbt seed

# 3. Build the foundational layer (Bronze & Silver)
dbt run --select staging intermediate

# 4. Build independent dimension tables (e.g., dim_skill)
dbt run --select dim_skill

# 5. Build Fact tables and downstream dependencies (the + sign automatically captures dependent dims and bridge tables)
dbt run --select fct_job_postings+

# 6. Execute SCD2 historical snapshot tracking
dbt snapshot

# 7. Run full-pipeline data quality tests (including Generic and Singular Tests)
dbt test
```

**Design Consideration**: Splitting `dbt run` into independent modules prevents the entire pipeline from blindly rerunning due to a failure in a single core fact table, significantly improving cloud debugging efficiency.

## 11. Full-Pipeline Volume Audit (Data Lineage Check)

To verify the health and completeness of the data pipeline, we performed a full-table row count audit across all layers (Raw ➔ Bronze ➔ Silver ➔ Gold). The results perfectly demonstrate the data cleaning funnel and the robustness of the deduplication logic:

| Layer | Table Name | Total Rows |
| :--- | :--- | :--- |
| **Raw Layer** | job_postings | 843,097 |
| **Raw Layer** | raw_job_skills | 3,304,574 |
| **Bronze Layer** | stg_job_postings | **491,140** |
| **Bronze Layer** | stg_job_skills | 3,304,574 |
| **Silver Layer** | int_skill_demand | 960 |
| **Gold Layer** | fct_job_postings | **491,140** |
| **Gold Layer** | dim_company | 65,970 |
| **Gold Layer** | dim_skill | 1,422 |
| **Gold Layer** | bridge_job_skills | 3,304,574 |
| **Snapshot Layer** | scd_job_postings | **491,140** |

**Audit Insights & Validation**:
1. **Data Cleansing (Raw ➔ Bronze)**: The raw `job_postings` dataset starts with **843,097** rows. Through strict deduplication (`row_number() = 1`) and null-key filtering, the Bronze layer accurately reduces this to **491,140** unique jobs, eliminating 352k rows of dirty data.
2. **Grain Consistency (Bronze ➔ Fact ➔ Snapshot)**: The `fct_job_postings` (Fact table) and the initial `scd_job_postings` (Snapshot) exactly match the Bronze layer at **491,140** rows, proving absolutely zero data inflation (Fan-out) and strict 1 row per job.
3. **Dimension Convergence (Gold)**: The company dimension collapses to 65,970 unique companies, and the skill dimension represents 1,422 core skills. The intermediate `int_skill_demand` pre-aggregates these into 960 in-demand skills before joining.
4. **Bridge Expansion**: The `bridge_job_skills` retains the full **3.3 million+** relationship details, ready for BI roll-ups without polluting the Fact table's grain.

## 12. References & Acknowledgements

The architecture inspiration, original dataset, and part of the business logic of this project are referenced from the following open-source project and tutorial. Special thanks:

- **Original Dataset**: 
  [lukebarousse/dbt_Analytics_Engineering_Course/releases](https://github.com/lukebarousse/dbt_Analytics_Engineering_Course/releases)
- **Tutorial Video**: 
  [YouTube - Analytics Engineering Course](https://www.youtube.com/watch?v=1Bf049kXV2g)

