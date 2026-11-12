with job_skills as (select * from {{ ref('stg_job_skills') }})

select skill_id, count(distinct job_id) as postings_with_skill
from job_skills
group by skill_id
