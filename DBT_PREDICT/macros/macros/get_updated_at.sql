/* The purpose of this set of macros is to add fields UPDATED_AT_UTC and UPDATED_AT_LOCAL on several models */

{% macro updated_at_fields() %}

    /*
        The purpose of this macro is to add the two fields to the model for writing them in tables
    */

    '{{ run_started_at }}' AS UPDATED_AT_UTC,
    convert_timezone('UTC', lt.TIMEZONE, '{{ run_started_at }}') as UPDATED_AT_LOCAL

{% endmacro %}

{% macro updated_at_table_join_season(source_table) %}

    /*
        The purpose of this macro is to join a model with local_time seed via the season model
        in order to calculate the two fields
        It needs to have SEASON_KEY field in input in the source_table to join
        Args:
            source_table (str): the alias of cte calling the macro inside the model
    */
    LEFT JOIN
        {{ ref('curated_season')}} season_lt
        ON {{source_table}}.SEASON_KEY = season_lt.SEASON_KEY
    LEFT JOIN
        {{ ref('local_time')}} lt 
        ON lt.COUNTRY = season_lt.SEASON_COUNTRY
{% endmacro%}


{% macro updated_at_table_join_forum(source_table) %}

    /*
        The purpose of this macro is to join a model with local_time seed via the forum model
        in order to calculate the two fields
        It needs to have FORUM_KEY field in input in the source_table to join
        Args:
        source_table (str): the alias of cte calling the macro inside the model
    */
    LEFT JOIN
        {{ ref('curated_forum')}} forum_lt
        ON {{source_table}}.FORUM_KEY = forum_lt.FORUM_KEY
    LEFT JOIN
        {{ ref('local_time')}} lt 
        ON lt.COUNTRY = forum_lt.FORUM_COUNTRY
{% endmacro%}