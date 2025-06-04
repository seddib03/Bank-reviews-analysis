WITH base AS (
    SELECT DISTINCT
        city,
        location
    FROM {{ ref('fct_sentiment_analysis') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['city', 'location']) }} AS location_id,
    city,
    location
FROM base
