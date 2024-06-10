{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro exclude_column_versions(columns, exclude_versions) %}
  {# 
  columns: an array of column names
  exclude_versions: an array of versions (more generically a suffix) to exclude columns that end with this value
  returns: filtered array of columns that do not end with any of the exclude versions.
  #}
  {% if not exclude_versions %}
    {{ return(columns) }}
  {% endif %}

  {%- set filtered_columns_by_version = [] -%}

  {%- set columns_to_exclude = [] -%}

  {% for column in columns %}
  {# Remove columns that end with the version we want to exclude #}
    {% for version in exclude_versions %}
      {% if column.name.endswith(version) %}
        {% do columns_to_exclude.append(column) %}
      {% endif %}
    {% endfor %}
    {% if not column in columns_to_exclude %}
        {% do filtered_columns_by_version.append(column) %}
    {% endif %}
  {% endfor %}
  {{ return(filtered_columns_by_version) }}

{% endmacro %}
