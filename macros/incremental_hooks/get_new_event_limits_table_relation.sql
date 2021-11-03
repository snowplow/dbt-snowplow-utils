{# Returns the new events limits table reference. 
This table contains lower and upper tstamp limits of the current run #}

{% macro get_new_event_limits_table_relation(package_name) %}

  {%- set new_event_limits_table = ref(package_name~'_base_new_event_limits') -%}

  {{ return(new_event_limits_table) }}

{% endmacro %}
