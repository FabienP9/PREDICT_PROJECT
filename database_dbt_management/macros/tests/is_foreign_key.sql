/* 
    The purpose of this test is to assess the validity of the relationship 
    between a not null foreign key, and its table primary key related
    Inputs:
        model: the name of the model of the foreign key
        column_name: the name of the foreign key column
        to_model: the name of the model of the primary key related
        to_column: the name of the primary key column
*/

{% test is_foreign_key(model,column_name,to_model,to_column) %}
    SELECT 
        *
    FROM 
        {{ model }}
    WHERE 
        {{ column_name }} IS NULL
        OR NOT EXISTS (
            SELECT 1
            FROM {{ to_model }} target
            WHERE target.{{ to_column }} = {{ model }}.{{ column_name }}
        )

{% endtest %}