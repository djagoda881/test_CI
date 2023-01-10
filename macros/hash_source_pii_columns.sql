{% macro hash_source_pii_columns(project, schema, table=None) -%}

    {%- set pii_columns = get_source_pii_columns(project=project, schema=schema, table=table) -%}

    {%- for column in pii_columns %}
        {{ "md5(cast(" ~ column ~ " as string))" }} as {{ column }}, {{ "\n" }}
    {%- endfor -%}
        {{ dbt_utils.star(from=source(schema, table), except=pii_columns) }}

{%- endmacro %}
