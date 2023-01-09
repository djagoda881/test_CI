{# Adapted from dbt-codegen #}


---
{# Generate seed YAML file and updates with seed information #}

{# 'seed_names' refers a list of seeds whose information will be appended to the YAML file #}


{% macro create_seed_yaml_text(
    seed_names=none,
    technical_owner=none,
    business_owner=none,
    new=False,
    schema_name=target.schema,
    database_name=target.database,
    generate_columns=True,
    include_descriptions=True,
    include_tags=False,
    name=schema_name
    ) %}

{% set seeds_yaml=[] %}

{% if new %}
    {% do seeds_yaml.append('version: 2') %}
    {% do seeds_yaml.append('') %}
    {% do seeds_yaml.append('seeds:') %}
{% endif %}


{% if seed_names is none %}
    {% set tables = codegen.get_tables_in_schema(schema_name, database_name, '*', '') %}
{% else %}
    {% set tables = seed_names %}
{% endif %}

{% for table in tables %}
    {% do seeds_yaml.append('  - name: ' ~ table | lower ) %}
    
    {% if include_descriptions %}
        {% do seeds_yaml.append('    description: ""' ) %}
    {% endif %}

    {% if include_tags %}
    {% do seeds_yaml.append('    tags: []' ) %}
    {% endif %}

    {% do seeds_yaml.append('    meta:' ) %}
    {% if technical_owner %}
        {% do seeds_yaml.append('      technical_owner: ' ~ technical_owner)%}
    {% else %}
        {% do seeds_yaml.append('      technical_owner: @Unassigned')%}
    {% endif %}
    {% if business_owner %}
        {% do seeds_yaml.append('      business_owner: ' ~ business_owner)%}
    {% else %}
        {% do seeds_yaml.append('      business_owner: @Unassigned')%}        
    {% endif %}


    {% if generate_columns %}
    {% do seeds_yaml.append('    columns:') %}

        {% set table_relation=api.Relation.create(
            database=database_name,
            schema=schema_name,
            identifier=table
        ) %}

        {% set columns=adapter.get_columns_in_relation(table_relation) %}

        {% for column in columns %}
            {% do seeds_yaml.append('      - name: ' ~ column.name | lower ) %}
            {% if include_descriptions %}
                {% do seeds_yaml.append('        description: ""' ) %}
            {% endif %}
            {% do seeds_yaml.append('        tests:' ) %}
            {% do seeds_yaml.append('          # - unique' ) %}
            {% do seeds_yaml.append('          # - not_null' ) %}
        {% endfor %}

    {% endif %}

{% endfor %}

{% if execute %}

    {% set joined = seeds_yaml | join ('\n') %}
    {{ print(joined) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}
