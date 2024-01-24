{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Returns the incremental manifest table reference.
This table contains 1 row/model with the latest tstamp consumed #}

{% macro get_incremental_manifest_table_relation(package_name) %}

  {%- set incremental_manifest_table = ref(package_name~'_incremental_manifest') -%}

  {{ return(incremental_manifest_table) }}

{% endmacro %}
