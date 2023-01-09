{# Adapted from dbt-codegen #}

{# Generate column YAML template #}
{% macro generate_column_yaml(column, model_yaml, columns_metadata_dict, parent_column_name="", include_pii_tag=True) %}

    {{ log("Generating YAML for column '" ~ column.name ~ "'...") }}

    {% if parent_column_name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% else %}
        {% set column_name = column.name %}
    {% endif %}

    {% set column_metadata_dict = columns_metadata_dict.get(column.name | lower, {}) %}

    {% if include_pii_tag %}
        {% set tags = column_metadata_dict.get("tags", []) %}
    {% else %}
        {% set tags = column_metadata_dict.get("tags", []) | reject("equalto", "PII") | list %}
    {% endif %}

    {% do model_yaml.append('      - name: ' ~ column.name | lower ) %}
    {% do model_yaml.append('        description: "' ~ column_metadata_dict.get("description", "") ~ '"') %}
    {% do model_yaml.append('        tests:' ) %}
    {% do model_yaml.append('            # - unique' ) %}
    {% do model_yaml.append('            # - not_null' ) %}
    {% do model_yaml.append('        tags: ' ~ tags ) %}
    {% do model_yaml.append('') %}

    {% if column.fields|length > 0 %}
        {% for child_column in column.fields %}
            {% set model_yaml = generate_column_yaml(child_column, model_yaml, column_metadata_dict, parent_column_name=column_name) %}
        {% endfor %}
    {% endif %}
    {% do return(model_yaml) %}
{% endmacro %}


{% macro generate_model_yaml(
    model_name,
    technical_owner=none,
    business_owner=none,
    upstream_metadata=True,
    include_sla=True,
    include_pii_tag=False
    ) %}
{# 
Generate model YAML template.

Args:
    model_name (str): The name of the model for which to generate the template.
    technical_owner (str, optional) the technical owner of the table.
    business_owner (str, optional) the business owner of the table.
    upstream_metadata (bool, optional): Whether to inherit upstream model metadata.
    include_sla (bool, optional): Whether to include the SLA meta key.
    include_pii_tag (bool, optional): Whether to include the PII tag.
    This may be useful when PII columns are already masked in the base table.
#}

{{ log("Generaling model YAML for model '" ~ model_name ~ "'...") }}

{% if upstream_metadata %}
    {% set upstream_model_metadata = get_parent_source_or_model_metadata(model_name) %}
{% else %}
    {% set upstream_model_metadata = {} %}
{% endif %}

{% set dependencies = get_model_dependencies(model_name) %}
{% set upstream_model_type = dependencies["type"] %}

{# Table metadata. #}
{% set model_yaml=[] %}
{% do model_yaml.append('version: 2') %}
{% do model_yaml.append('') %}
{% do model_yaml.append('models:') %}
{% do model_yaml.append('  - name: ' ~ model_name | lower) %}

{% if upstream_model_type == "source" %}
    {% do model_yaml.append('    description: Base model of the `' ~ model_name ~ "` table.") %}
{% else %}
    {% do model_yaml.append('    description: ""') %}
{% endif %}


{% do model_yaml.append('    meta:' ) %}
{% set metadata = upstream_model_metadata.get("meta", {}) %}

{% if technical_owner %}
    {% do model_yaml.append('      technical_owner: "' ~ technical_owner ~ '"')%}
{% else %}
    {% set technical_owner = metadata.get("technical_owner", "@Unassigned") %}
    {% do model_yaml.append('      technical_owner: ' ~ technical_owner ) %}
{% endif %}

{% if business_owner %}
    {% do model_yaml.append('      business_owner: ' ~ business_owner ) %}
{% else %}
    {% set business_owner = metadata.get("business_owner", "@Unassigned") %}
    {% do model_yaml.append('      business_owner: ' ~ business_owner ) %}
{% endif %}


{% if include_sla %}
    {% set sla = metadata.get("SLA", "24 hours") %}
    {% do model_yaml.append('      SLA: ' ~ sla ) %}
{% endif %}

{% do model_yaml.append('    columns:') %}

{% if upstream_model_type == "source" %}
    {% set schema = dependencies["node"].split('.')[-2] %}
    {% set relation=source(schema, model_name) %}
{% else %}
    {% set relation=ref(model_name) %}
{% endif %}

{%- set columns = adapter.get_columns_in_relation(relation) -%}

{# Column metadata. #}
{% set columns_metadata_dict = get_parent_source_or_model_column_metadata(model_name) if upstream_metadata else {} %}

{% for column in columns %}
    {% set model_yaml = generate_column_yaml(column, model_yaml, columns_metadata_dict, include_pii_tag=False) %}
{% endfor %}

{%- if execute -%}

    {%- set joined = model_yaml | join ('\n') -%}
    {{ print(joined) }}
    {%- do return(joined) -%}

{%- endif -%}

{%- endmacro -%}