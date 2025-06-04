WITH base AS (
    SELECT DISTINCT
        branch,
        bank
    FROM {{ ref('fct_sentiment_analysis') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['bank', 'branch']) }} AS branch_id,
    branch,
    bank
FROM base
