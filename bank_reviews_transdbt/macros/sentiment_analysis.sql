{% macro analyze_sentiment(column_name) %}
    CASE
        WHEN {{ column_name }} ILIKE '%excellent%' OR {{ column_name }} ILIKE '%good%' THEN 1
        WHEN {{ column_name }} ILIKE '%bad%' OR {{ column_name }} ILIKE '%poor%' THEN -1
        ELSE 0
    END
{% endmacro %}
