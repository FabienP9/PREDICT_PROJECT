/* The purpose of this table is to strictly copy data from curated_choice_team_check, on a incremental way
Will be run via Python program when it is manually decided in the Python program input 
that curated_choice_team_check is ok */
{{config(
    materialized="incremental",
    unique_key=['CHOICE_TEAM_KEY']
)}}
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we retrieve all columns (*) automatically as we want a strict copy
SELECT 
    * 
FROM 
    {{ref('curated_choice_team_check')}}
    {% if is_incremental() and max_updated_at is not none %}
    WHERE UPDATED_AT_UTC > '{{ max_updated_at }}'
    {% endif %}