{{ config(
    materialized = 'table'
) }}

with
    source as (
        select *
        from {{ source('raw_sources', 'job_postings') }}
        where error is not true and job_id is not null
    ),

    parsed as (
        select *, from_json(job_extensions_raw, 'array<string>') as job_extensions
        from source
    ),

    cleaned as (
        select
            -- ids + scrape metadata
            job_id,
            search_term,
            search_date,
            search_time as searched_at,
            search_location,

            -- posting attributes
            job_title,
            trim(company_name) as company_name,
            job_location,
            regexp_replace(job_via, '^via ', '') as source_platform,
            job_schedule_type,
            case
                when lower(trim(cast(job_work_from_home as string))) = 'true'
                then true
                when lower(trim(cast(job_work_from_home as string))) = 'false'
                then false
                else false
            end as job_work_from_home,
            job_posted_at,

            -- salary parsing
            job_salary,
            {{ parse_salary('min') }} as salary_min,
            {{ parse_salary('max') }} as salary_max,
            ({{ parse_salary('min') }} + {{ parse_salary('max') }}) / 2 as salary_avg,
            nullif(
                regexp_extract(job_salary, '(year|hour|month|day|week)', 1), ''
            ) as salary_period,
            case
                when job_salary is not null
                then
                    coalesce(
                        nullif(trim(regexp_extract(job_salary, '^([^0-9]+)', 1)), ''),
                        'USD'
                    )
            end as salary_currency,

            -- unpack extension boolean flags using macro
            {{ unpack_extensions('job_extensions', [
                'Health insurance', 
                'Dental insurance', 
                'Paid time off', 
                'No degree mentioned'
            ]) }},

            -- deduplication logic
            row_number() over (partition by job_id order by search_date desc) as row_num
        from parsed
    ),

    final as (
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
        from cleaned
        where row_num = 1
    )

select *
from final
