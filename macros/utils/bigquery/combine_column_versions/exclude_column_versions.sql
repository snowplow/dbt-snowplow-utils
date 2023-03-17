{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro exclude_column_versions(columns, exclude_versions) %}
  {%- set filtered_columns_by_version = [] -%}
  {% for column in columns %}
    {%- set col_version = column.name[-5:] -%}
    {% if col_version not in exclude_versions %}
      {% do filtered_columns_by_version.append(column) %}
    {% endif %}
  {% endfor %}

  {{ return(filtered_columns_by_version) }}

{% endmacro %}
