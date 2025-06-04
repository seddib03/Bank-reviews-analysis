-- models/staging/stg_reviews.sql

with raw as (
    select
        bank,
        city,
        branch,
        location,
        lower(regexp_replace(text, '[^\w\s]', '', 'g')) as clean_text,
        cast(rating as integer) as rating,
        date,
        row_number() over (partition by bank, branch, text, date order by created_at) as row_num
    from public.stg_customer_reviews
)

select *
from raw
where row_num = 1
