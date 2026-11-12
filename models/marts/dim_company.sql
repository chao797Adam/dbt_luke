with job_postings as (select * from {{ ref('fct_job_postings') }})

select
    company_name,
    count(*) as total_job_postings,
    -- conditional aggregation, not a WHERE: filtering the query would drop
    -- salary-less postings from total_job_postings. only like units average —
    -- USD yearly — the rest stay out rather than guessed
    round(
        avg(
            case
                when salary_currency = 'USD' and salary_period = 'year' then salary_avg
            end
        ),
        2
    ) as avg_salary_year
from job_postings
-- company_name is the natural key; a null row would break the one-row-per-company grain
where company_name is not null
group by company_name
