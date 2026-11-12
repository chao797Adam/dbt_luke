with
    source as (select * from {{ source('raw_sources', 'monthly_summary') }}),

    renamed as (
        select
            --
            cast(month as date) as summary_month,

            --
            cast(total_postings as integer) as total_postings,
            cast(remote_postings as integer) as remote_postings,
            cast(pct_remote as double) as pct_remote

        from source
    )

select *
from renamed
