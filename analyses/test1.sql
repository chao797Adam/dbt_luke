{{ config(
    materialized = 'table'
) }}

with
    source as (
        select *
        from {{ source('raw_sources', 'job_postings') }}
        where error is not true and job_id is not null
    ),
    final as (
        select
            *,
            row_number() over (partition by job_id order by search_date desc) as row_num
        from source
        where row_num = 1
    )
select *
from final
