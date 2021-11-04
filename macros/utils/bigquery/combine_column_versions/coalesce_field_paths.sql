{% macro coalesce_field_paths(paths, field_alias, include_field_alias, relation_alias) %}
  
  {% set relation_alias = '' if relation_alias is none else relation_alias~'.' %}

  {% set field_alias = '' if not include_field_alias else ' as '~field_alias %}

  {% set joined_paths = relation_alias~paths|join(', '~relation_alias) %}

  {% set coalesced_field_paths = 'coalesce('~joined_paths~')'~field_alias %}

  {{ return(coalesced_field_paths) }}

{% endmacro %}
