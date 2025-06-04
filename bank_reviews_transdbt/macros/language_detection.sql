-- macros/language_detection.sql
{% macro detect_language(column_name) %}
    CASE
        WHEN lower({{ column_name }}) ~ 'le|la|les|mais|donc' THEN 'fr'
        WHEN lower({{ column_name }}) ~ 'the|and|but|this|that' THEN 'en'
        WHEN lower({{ column_name }}) ~ 'و|في|إلى' THEN 'ar'
        ELSE 'unknown'
    END
{% endmacro %}