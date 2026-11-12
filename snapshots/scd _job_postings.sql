{% snapshot scd_job_postings %}

    {{
    config(
      target_schema='snapshots',        
      unique_key='job_id',
      strategy='check',
      check_cols=[
        'search_term',
        'search_date',
        'search_location',
        'job_title',
        'company_name',
        'job_location',
        'source_platform',
        'job_schedule_type',
        'job_work_from_home',
        'job_posted_at',
        'job_salary',
        'salary_min',
        'salary_max',
        'salary_avg',
        'salary_period',
        'salary_currency',
        'has_health_insurance',
        'has_dental_insurance',
        'has_paid_time_off',
        'has_no_degree_mentioned'
      ]
    )
}}

    select *
    from {{ ref('stg_job_postings') }}

{% endsnapshot %}
