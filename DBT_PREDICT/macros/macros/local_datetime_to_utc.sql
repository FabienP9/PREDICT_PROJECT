/* The purpose of this module is to transform local date or time or both to utc equivalent */
{% macro local_datetime_to_utc(source_table, date_column, time_column, object_type) %}

    /* This macro is transforming to utc depending of the presence or not of a time
    Args:
        source_table: the cte calling the macro
        date_column: the local date column
        time_column: the local time column (can be null)
        object_type: the utc object we return: date or time or datetime
    */
CASE    
    WHEN {{ source_table }}.{{ time_column }} IS NULL 
    THEN 
        /* if the input time is null we return the same date or the same time as input */
        {% if object_type == 'DATE' %} 
            {{ source_table }}.{{ date_column }}
        {% else %} --object_type == 'TIME' with TIME NULL
            NULL
        {% endif %}
    /* else we concat date and time, we convert to utc, and then we cast to return either the date or the time or both of them*/
    ELSE  CAST(convert_timezone(lt.TIMEZONE, 'UTC',
            TO_TIMESTAMP(TO_CHAR({{ source_table }}.{{ date_column }}, 'YYYY-MM-DD') || ' ' ||
            TO_CHAR({{ source_table }}.{{ time_column }}, 'HH24:MI:SS'))) AS {{object_type}})
END
{% endmacro %}
