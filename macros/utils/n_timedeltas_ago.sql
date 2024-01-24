{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro n_timedeltas_ago(n, timedelta_attribute) %}

  {% set arg_dict = {timedelta_attribute: n} %}
  {% set now = modules.datetime.datetime.now() %}
  {% set n_timedeltas_ago = (now - modules.datetime.timedelta(**arg_dict)) %}

  {{ return(n_timedeltas_ago) }}

{% endmacro %}
