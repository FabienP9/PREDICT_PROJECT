/* 
    The purpose of this macro is to create tables in custom schema
    Inputs:
        custom_schema_name: the name of the schema where we create the table
*/

{% macro generate_schema_name(custom_schema_name, node) %}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{% endmacro %}
