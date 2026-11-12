with
    source as (select * from {{ source('raw_sources', 'raw_job_skills') }}),

    cleaned as (
        select
            trim(job_id) as job_id,
            trim(skill_id) as skill_id,
            trim(skill_keyword) as skill_keyword
        from source
        where job_id is not null and skill_id is not null
    )

select job_id, skill_id, skill_keyword
from cleaned
