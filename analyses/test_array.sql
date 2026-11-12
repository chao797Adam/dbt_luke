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
from parsed
limit 10
