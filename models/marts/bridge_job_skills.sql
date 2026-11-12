with
    job_skills as (
        select job_id, skill_id, skill_keyword from {{ ref('stg_job_skills') }}
    ),

    job_postings as (select job_id from {{ ref('fct_job_postings') }})

select
    p.job_id,  -- Using the fact table's ID as the source of truth
    s.skill_id,
    s.skill_keyword
from job_skills s
inner join job_postings p on s.job_id = p.job_id
