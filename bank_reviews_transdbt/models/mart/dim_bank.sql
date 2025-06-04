WITH base AS (
    SELECT DISTINCT
        bank
    FROM {{ ref('fct_sentiment_analysis') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bank']) }} AS bank_id,
    bank
FROM base
