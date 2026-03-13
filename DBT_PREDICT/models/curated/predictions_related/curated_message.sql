/* The purpose of this table is to strictly copy data from curated_message_check, on a incremental way
Will be run via Python program when it is manually decided in Python program input 
that curated_message_check is ok */

{{config(
    materialized="incremental",
    unique_key=['MESSAGE_EDITION_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(DBT_VALID_FROM) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}

-- we retrieve all columns (*) automatically as we want a strict copy
SELECT 
    * 
FROM 
    {{ref('curated_message_check')}}
    {% if is_incremental() and max_updated_at is not none %}
    WHERE DBT_VALID_FROM > '{{ max_updated_at }}'
    {% endif %}