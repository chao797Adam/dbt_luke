{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='job_id',
        incremental_predicates = [
            "DBT_INTERNAL_DEST.search_date >= date_sub(current_date(), 2)"
        ]
    )
}}

with
    incremental_source as (
        select *
        from {{ ref('stg_job_postings') }}

        {% if is_incremental() %}
            where search_date >= (select date_sub(max(search_date), 2) from {{ this }})
        {% endif %}
    ),

    deduplicated_source as (
        select *
        from incremental_source
        qualify
            row_number() over (
                partition by job_id order by search_date desc, searched_at desc
            )
            = 1
    )

select
    job_id,
    search_term,
    search_date,
    searched_at,
    search_location,
    job_title,
    company_name,
    job_location,
    source_platform,
    job_schedule_type,
    job_work_from_home,
    job_posted_at,
    job_salary,
    salary_min,
    salary_max,
    salary_avg,
    salary_period,
    salary_currency,
    has_health_insurance,
    has_dental_insurance,
    has_paid_time_off,
    has_no_degree_mentioned
from deduplicated_source
