WITH base AS (
    SELECT *
    FROM {{ ref('fct_sentiment_analysis') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bank', 'branch', 'location', 'date', 'clean_text']) }} AS review_id,
    {{ dbt_utils.generate_surrogate_key(['bank']) }} AS bank_id,
    {{ dbt_utils.generate_surrogate_key(['bank', 'branch']) }} AS branch_id,
    {{ dbt_utils.generate_surrogate_key(['city', 'location']) }} AS location_id,
    {{ dbt_utils.generate_surrogate_key(['sentiment_category']) }} AS sentiment_id,
    clean_text,
    rating,
    date,
    language,
    sentiment_score
FROM base
