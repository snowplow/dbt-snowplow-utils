{# Returns the incremental manifest table reference. 
This table contains 1 row/model with the latest tstamp consumed #}

{% macro get_incremental_manifest_table_relation(package_name) %}

  {%- set incremental_manifest_table = ref(package_name~'_incremental_manifest') -%}

  {{ return(incremental_manifest_table) }}

{% endmacro %}
