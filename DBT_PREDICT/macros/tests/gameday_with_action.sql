/* 
    There can be maximum one gameday with action during a run
    The purpose of this test is to check that.
    It is used for the tables curated_gameday and consumpted_gameday, where a row = a gameday
    Inputs:
        model: the name of the model on which the test checks
        fields: the list of columns to check if there is action
*/
{% test gameday_with_action(model, fields) %}

with validation_errors as (
    select *
    from {{ model }}
    where (
    {% for field in fields %}
        coalesce({{ field.name }}, 0)
        {% if not loop.last %} + {% endif %}
    {% endfor %}
    ) != 0
)
select
    count(*) as error_count
from validation_errors
having count(*) > 1 

{% endtest %}