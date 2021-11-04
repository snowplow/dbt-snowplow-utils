{% macro merge_fields_across_col_versions(fields_by_col_version) %}

  {# Flatten nested list of dicts into single list #}
  {% set all_cols = fields_by_col_version|sum(start=[]) %}

  {% set all_field_names = all_cols|map(attribute="field_name")|list %}

  {% set unique_field_names = all_field_names|unique|list %}

  {% set merged_fields = [] %}

  {% for field_name in unique_field_names %}

    {# Get all field_paths per field. Returned as array. #}
    {% set field_paths = all_cols|selectattr('field_name','equalto', field_name)|map(attribute='path')|list %}
    
    {# Get nested_level of field. Returned as single element array. #}
    {% set nested_level = all_cols|selectattr('field_name',"equalto", field_name)|map(attribute='nested_level')|list%}

    {% set merged_field = {
                           'field_name': field_name,
                           'field_paths': field_paths,
                           'nested_level': nested_level[0]
                           } %}

    {% do merged_fields.append(merged_field) %}

  {% endfor %}

  {{ return(merged_fields) }}

{% endmacro %}
