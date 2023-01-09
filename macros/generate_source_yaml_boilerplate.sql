{# Adapted from dbt-codegen #}

{% macro get_tables_in_schema(schema_name, database_name=target.database, table_pattern='%', exclude='') %}

    {% set tables=dbt_utils.get_relations_by_pattern(
        schema_pattern=schema_name,
        database=database_name,
        table_pattern=table_pattern,
        exclude=exclude
    ) %}

    {% set table_list= tables | map(attribute='identifier') %}

    {{ return(table_list | sort) }}

{% endmacro %}


---
{% macro generate_source(
    schema_name,
    technical_owner=none,
    business_owner=none,
    database_name=target.database,
    generate_columns=True,
    include_descriptions=True,
    include_table_profiling=True,
    include_sla=True,
    include_freshness=True,
    loaded_at_field="_viadot_downloaded_at_utc::timestamp",
    freshness={
        "warn_after": "{count: 24, period: hour}",
        "error_after": "{count: 48, period: hour}"
        },
    table_pattern='*',
    exclude='',
    name=schema_name,
    table_names=None
    ) %}


{% set sources_yaml=[] %}

{% if table_names is none %}
    {% do sources_yaml.append('version: 2') %}
    {% do sources_yaml.append('') %}
    {% do sources_yaml.append('sources:') %}
    {% do sources_yaml.append('  - name: ' ~ name | lower) %}

    {% if database_name != target.database %}
        {% do sources_yaml.append('    database: ' ~ database_name | lower) %}
    {% endif %}

    {% do sources_yaml.append('    schema: ' ~ schema_name | lower) %}
    {% if include_descriptions %}
        {% do sources_yaml.append('    description: ""' ) %}
    {% endif %}
    {% do sources_yaml.append('\n    tables:') %}

    {% set tables=codegen.get_tables_in_schema(schema_name, database_name, table_pattern, exclude) %}
{% else %}
    {% set tables = table_names %}
{% endif %}

{% if table_names %}
    {% do sources_yaml.append('') %}
{% endif %}

{% for table in tables %}
    {% do sources_yaml.append('      - name: ' ~ table | lower ) %}
    
    {% if include_descriptions %}
        {% do sources_yaml.append('        description: |') %}
    {% endif %}
        {% if include_table_profiling %}
            {# Note that the doc must already exist. You can generate it beforehand wiht dbt-profiler. #}
            {% do sources_yaml.append('          {{ doc("' ~ schema_name ~ '_' ~ table ~ '") }}') %}
        {% endif %}
    

    {% if include_freshness %}
        {% do sources_yaml.append('        loaded_at_field: ' ~ loaded_at_field ) %}
        {% do sources_yaml.append('        freshness:' ) %}
        {% do sources_yaml.append('          warn_after: ' ~ freshness.get("warn_after", "") ) %}
        {% do sources_yaml.append('          error_after: ' ~ freshness.get("error_after", "") ) %}
    {% endif %}

    {% do sources_yaml.append('        tags: []' ) %}


    {% do sources_yaml.append('    meta:' ) %}
    {% if technical_owner %}
        {% do sources_yaml.append('      technical_owner: ' ~ technical_owner)%}
    {% else %}
        {% do sources_yaml.append('      technical_owner: @Unassigned')%}
    {% endif %}
    {% if business_owner %}
        {% do sources_yaml.append('      business_owner: ' ~ business_owner)%}
    {% else %}
        {% do sources_yaml.append('      business_owner: @Unassigned')%}        
    {% endif %}


    {% if include_sla %}
        {% do sources_yaml.append('          SLA: "24 hours"' ) %}
    {% endif %}

    {% if generate_columns %}
    {% do sources_yaml.append('        columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=adapter.get_columns_in_relation(table_relation) %}

        {% for column in columns %}
            {% do sources_yaml.append('          - name: ' ~ column.name | lower ) %}
            {% if include_descriptions %}
                {% do sources_yaml.append('            description: ""' ) %}
            {% endif %}
            {% do sources_yaml.append('            tags: []' ) %}
        {% endfor %}

    {% endif %}

{% endfor %}


{% if execute %}

    {% set joined = sources_yaml | join ('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}
