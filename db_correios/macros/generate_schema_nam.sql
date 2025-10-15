{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
      Sobrescreve o comportamento padrão do dbt 
      para não concatenar o schema base do perfil (ex: public_)
    #}
    {% if custom_schema_name is none %}
        {{ target.schema }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}
{%- endmacro %}
