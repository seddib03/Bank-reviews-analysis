WITH base AS (
    SELECT DISTINCT
        sentiment_category
    FROM {{ ref('fct_sentiment_analysis') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['sentiment_category']) }} AS sentiment_id,
    sentiment_category
FROM base
