with
    source as (
        select *
        from {{ source('raw_sources', 'job_postings') }}
        where error is not true and job_id is not null
    ),

    parsed as (
        select *, from_json(job_extensions_raw, 'array<string>') as job_extensions
        from source
    )
select
    job_id,
    job_title,
    job_extensions,
    {{ unpack_extensions('job_extensions', ['Health insurance', 'Dental insurance', 'Paid time off', 'No degree mentioned']) }}
    ,
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
                nullif(trim(regexp_extract(job_salary, '^([^0-9]+)', 1)), ''), 'USD'
            )
    end as salary_currency
from parsed
where job_salary is not null
limit 10
