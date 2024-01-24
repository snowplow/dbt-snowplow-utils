{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Returns the new events limits table reference.
This table contains lower and upper tstamp limits of the current run #}

{% macro get_new_event_limits_table_relation(package_name) %}

  {%- set new_event_limits_table = ref(package_name~'_base_new_event_limits') -%}

  {{ return(new_event_limits_table) }}

{% endmacro %}
