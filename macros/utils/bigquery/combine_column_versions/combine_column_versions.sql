{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro combine_column_versions(relation, column_prefix, required_fields=[], nested_level=none, level_filter='equalto', relation_alias=none, include_field_alias=true, array_index=0, max_nested_level=15, exclude_versions=[]) %}

  {# Create field_alias if not supplied i.e. is not tuple #}
  {% set required_fields_tmp = required_fields %}
  {% set required_fields = [] %}
  {% for field in required_fields_tmp %}
    {% set field_tuple = snowplow_utils.get_field_alias(field) %}
    {% do required_fields.append(field_tuple) %}
  {% endfor %}

  {% set required_field_names = required_fields|map(attribute=0)|list %}

  {# Determines correct level_limit. This limits recursive iterations during unnesting. #}
  {% set level_limit = snowplow_utils.get_level_limit(nested_level, level_filter, required_field_names) %}

  {# Limit level_limit to max_nested_level if required #}
  {% set level_limit = max_nested_level if level_limit is none or level_limit > max_nested_level else level_limit %}

  {%- set matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(relation, column_prefix) -%}

  {# Removes excluded versions, technically removes any column with that suffix #}
  {%- set filter_columns_by_version = snowplow_utils.exclude_column_versions(matched_columns, exclude_versions) -%}

  {%- set flattened_fields_by_col_version = [] -%}

  {# Flatten fields within each column version. Returns nested arrays of dicts. #}
  {# Dict: {'field_name': str, 'field_alias': str, 'flattened_path': str, 'nested_level': int #}
  {% for column in filter_columns_by_version|sort(attribute='name', reverse=true) %}
    {% set flattened_fields = snowplow_utils.flatten_fields(fields=column.fields,
                                                            parent=column,
                                                            path=column.name,
                                                            array_index=array_index,
                                                            level_limit=level_limit
                                                            ) %}

    {% do flattened_fields_by_col_version.append(flattened_fields) %}

  {% endfor %}

  {# Flatten nested arrays and merges fields across col version. Returns array of dicts containing all field_paths for field. #}
  {# Dict: {'field_name': str, 'flattened_field_paths': str, 'nested_level': int #}
  {% set merged_fields = snowplow_utils.merge_fields_across_col_versions(flattened_fields_by_col_version) %}

  {# Filters merged_fields based on required_fields if provided, or the level filter if provided. Default return all fields. #}
  {% set matched_fields = snowplow_utils.get_matched_fields(fields=merged_fields,
                                                            required_field_names=required_field_names,
                                                            nested_level=nested_level,
                                                            level_filter=level_filter
                                                            ) %}

  {% set coalesced_field_paths = [] %}

  {% for field in matched_fields %}

    {% set passed_field_alias = required_fields|selectattr(0, "equalto", field.field_name)|map(attribute=1)|list %}
    {% set default_field_alias = field.field_name|replace('.', '_') %}
    {# Use passed_field_alias from required_fields if supplied #}
    {% set field_alias = default_field_alias if not passed_field_alias|length else passed_field_alias[0] %}

    {# Coalesce each field's path across all version of columns, ordered by latest col version. #}
    {% set coalesced_field_path = snowplow_utils.coalesce_field_paths(paths=field.field_paths,
                                                                      field_alias=field_alias,
                                                                      include_field_alias=include_field_alias,
                                                                      relation_alias=relation_alias) %}

    {% do coalesced_field_paths.append(coalesced_field_path) %}

  {% endfor %}

  {# Returns array of all coalesced field paths #}
  {{ return(coalesced_field_paths) }}

{% endmacro %}
