select * from {{ ref('stg_job_postings') }} where salary_min < 0 or salary_max < 0
