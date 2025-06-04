-- models/transform/fct_reviews.sql
WITH base AS (
    SELECT * 
    FROM {{ ref('stg_reviews') }}
),

lang_detection AS (
    SELECT
        *,
        {{ detect_language('clean_text') }} AS language
    FROM base
)

SELECT * 
FROM lang_detection
