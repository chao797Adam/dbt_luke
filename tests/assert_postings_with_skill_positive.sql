select * from {{ ref('int_skill_demand') }} where postings_with_skill <= 0
