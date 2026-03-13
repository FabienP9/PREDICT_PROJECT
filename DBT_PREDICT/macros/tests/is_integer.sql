/* 
    The purpose of this test is to assess if a column is strictly an integer
    Inputs:
        model: the name of the model of the foreign key
        column_name: the name of the column which should be an integer
*/

{% test is_integer(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE 
        {{ column_name }} IS NULL
        OR {{ column_name }} != TRY_CAST({{ column_name }} AS INT)
{% endtest %}
