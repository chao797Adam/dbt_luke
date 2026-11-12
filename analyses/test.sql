%s ql
with
    job_postings as (
        select job_id
        from {{ ref('fct_job_postings') }}
        where search_term = 'Data Engineer'
    ),

    bridge as (select * from {{ ref('bridge_job_skills') }}),

    skills as (select * from {{ ref('dim_skill') }}),

    filtered_bridge as (
        select b.skill_id, b.job_id
        from bridge b
        inner join job_postings p on b.job_id = p.job_id
    )

select
    s.display_name,
    s.category,
    count(distinct f.job_id) as demand_count,
    round(
        count(distinct f.job_id)
        * 100.0
        / (select count(distinct job_id) from job_postings),
        2
    ) as percentage
from filtered_bridge f
inner join skills s on f.skill_id = s.skill_id
group by 1, 2
order by demand_count desc
limit 10
;
