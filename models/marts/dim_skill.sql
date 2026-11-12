with
    skill_categories as (select * from {{ ref('skill_categories') }}),

    observations as (select * from {{ ref('int_skill_demand') }})

select
    skill_id,
    display_name,
    category,
    subcategory,
    -- Default to 0 if no jobs require this skill
    coalesce(postings_with_skill, 0) as postings_with_skill
from skill_categories
left join observations using (skill_id)
order by postings_with_skill desc
