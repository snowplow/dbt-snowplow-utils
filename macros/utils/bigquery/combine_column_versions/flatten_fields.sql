{% macro flatten_fields(fields, parent, path, array_index, level_limit=none, level_counter=1, flattened_fields=[], field_name='', column_mode=none) %}
  
  {% for field in fields %}

    {# Only recurse up-until level_limit #}
    {% if level_limit is not none and level_counter > level_limit %}
      {{ return(flattened_fields) }}
    {% endif %}

    {# If parent column is an array then take element [array_index].  #}
    {# If we do not decide to override the behaviour, then we still use safe_offset. Otherwise we use . #}
    {% set behaviour_bool = (parent.mode == 'REPEATED' and (column_mode == 'REPEATED' or column_mode is none))  %}
    {% set delimiter = '[safe_offset(%s)].'|format(array_index) if behaviour_bool else '.' %}
    {% set path = path~delimiter~field.name %}
    {% set field_name = field_name~'.'~field.name if field_name != '' else field_name~field.name %}

    {% set field_dict = {
                         'field_name': field_name,
                         'path': path,
                         'nested_level': level_counter
                         } %}

    {% do flattened_fields.append(field_dict) %}

    {# If field has nested fields recurse to extract all fields, unless array. #}
    {% if field.dtype == 'RECORD' and field.mode != 'REPEATED' %}

      {{ snowplow_utils.flatten_fields(
                                  fields=field.fields,
                                  parent=field,
                                  level_limit=level_limit,
                                  level_counter=level_counter+1,
                                  path=path,
                                  flattened_fields=flattened_fields,
                                  field_name=field_name
                                  ) }}

    {% endif %}

  {% endfor %}

  {{ return(flattened_fields) }}

{% endmacro %}
