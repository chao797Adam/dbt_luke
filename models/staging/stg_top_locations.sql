with
    source as (select * from {{ source('raw_sources', 'top_locations') }}),

    renamed as (
        select
            trim(job_location) as job_location,
            cast(postings as integer) as posting_count
        from source
    )

select *
from renamed
