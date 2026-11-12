with
    source as (select * from {{ source('raw_sources', 'jobs_per_month') }}),

    renamed as (
        select
            --
            -- date_trunc
            cast(month as date) as month_year,
            trim(role) as job_role,
            cast(postings as integer) as posting_count

        from source
    )

select *
from renamed
