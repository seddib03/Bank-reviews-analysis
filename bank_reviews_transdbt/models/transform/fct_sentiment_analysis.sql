-- models/transform/fct_sentiment_analysis.sql
with base as (
    select * from {{ ref('fct_reviews') }}
),

sentiment as (
    select
        *,
        {{ analyze_sentiment('clean_text') }} as sentiment_score,
        case
            when {{ analyze_sentiment('clean_text') }} > 0.3 then 'Positive'
            when {{ analyze_sentiment('clean_text') }} < -0.3 then 'Negative'
            else 'Neutral'
        end as sentiment_category
    from base
)

select * from sentiment
