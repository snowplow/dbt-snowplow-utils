{# Finds matching columns from relationship based on column prefix. Returns array of column objects #}
{% macro get_columns_in_relation_by_column_prefix(relation, column_prefix) %}

  {# Prevent introspective queries during parsing #}
  {%- if not execute -%}
    {{ return('') }}
  {% endif %}

  {%- set columns = adapter.get_columns_in_relation(relation) -%}

  {%- set matched_columns = [] -%}

  {# add columns with matching prefix to matched_columns #}
  {% for column in columns %}
    {% if column.name.startswith(column_prefix) %}
      {% do matched_columns.append(column) %}
    {% endif %}
  {% endfor %}

  {% if matched_columns|length %}
    {{ return(matched_columns) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Snowplow: No columns found with prefix "~column_prefix) }}
  {% endif %}

{% endmacro %}

{# returns full path of a first level field within a given record column
   TODO: Improve unpacking of REPEATED mode columns. Dont just take 1st element. #}
{% macro get_record_field_path(column, field) %}
  
  {% if column.dtype == 'RECORD' %}
    {% if column.mode == 'NULLABLE' %}
      {% set field_path = column.name~'.'~field.name %}
    {% elif column.mode == 'REPEATED' %}
      {% set field_path = column.name~'[safe_offset(0)].'~field.name %}
    {% endif %}
    {{ return(field_path) }}
  {% else %}
    {{ exceptions.raise_compiler_error("Snowplow: Column is not of data type 'RECORD'") }}
  {% endif %}
  
{% endmacro %}

{#- returns coalesce of all field paths -#}
{% macro coalesce_field_paths(field_name, field_paths_array) %}
  coalesce({{ field_paths_array|join(', ') }}) as {{ field_name }}
{%- endmacro %}

{# BQ ONLY: Coalesces fields within a column of RECORD dtype, across differing versions of the column i.e. a_1, a_2..
   Can handle RECORD columns comprised of array of structs or a struct.
   Returns array of coalesced fields ordered by col version e.g. ['coalesce(a_2.id, a_1.id) as id', 'coalesce(a_2.name, a_1.name) as name']
   Only returns first level fields. Does not unpack nested fields. #}

{% macro combine_column_versions(relation, column_prefix, source_fields=none, renamed_fields=none, relation_alias=none) %}

  {# Prevent introspective queries during parsing #}
  {%- if not execute -%}
    {{ return('') }}
  {% endif %}

  {% if renamed_fields is not none %}
    {% if source_fields is none %}
      {{ exceptions.raise_compiler_error("Snowplow: To rename fields, pass source_fields arg") }}
    {% elif source_fields|length != renamed_fields|length %}
      {{ exceptions.raise_compiler_error("Snowplow: source_fields and rename_field lists length do not match") }}
    {% endif %}
  {% endif %}

  {%- set matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(relation, column_prefix) -%}
  
  {%- set first_level_fields = {} -%}

  {# create dictionary of all fields from matched_columns #}
  {# key: field_name, values: [paths]. Values ordered by column version number #}
  {% for column in matched_columns|sort(attribute='name', reverse=true) %}
    {% for field in column.fields %}
      {% set field_name = field.name %}
      {# get existing paths, default to empty array #}
      {% set paths = first_level_fields[field_name]|default([]) %}
      {# find path for current col version #}
      {% set path_to_add = snowplow_utils.get_record_field_path(column, field) %}
      {# if relation_alias passed, then prefix to path #}
      {% set path_to_add = relation_alias~'.'~path_to_add if relation_alias is not none else path_to_add %}
      {% do paths.append(path_to_add) %}
      {% do first_level_fields.update({field_name: paths}) %}
    {% endfor %}
  {% endfor %}

  {# if source_fields not passed set target_fields to all returned fields in col, else only source_fields #}
  {% if source_fields is none %}
    {% set target_fields = first_level_fields.keys() %}
  {% else %}
    {% set target_fields = source_fields %}
  {% endif %}

  {% set target_fields_paths_coalesce = [] %}
  
  {# Iterate target fields, coalesce paths, rename and append to fields_paths_coalesce. Preserves order of source_fields #}
  {% for field in target_fields %}
    {% set name_field_as = renamed_fields[loop.index0] if renamed_fields is not none else field %}
    {% if field not in first_level_fields %}
       {% do exceptions.warn("Snowplow: "~field~" not found in column's with prefix "~column_prefix) %}
    {% else %}
      {% set field_paths_coalesce = snowplow_utils.coalesce_field_paths(name_field_as, first_level_fields[field]) %}
      {% do target_fields_paths_coalesce.append(field_paths_coalesce) %}
    {% endif %}
  {% endfor %}

  {# return array of coalesced field paths #}
  {{ return(target_fields_paths_coalesce) }}

{% endmacro %}
