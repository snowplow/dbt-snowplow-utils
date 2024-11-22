{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro event_name_filter(event_names) %}
  {%- if event_names|length -%}
  ( 
    1=0
    {%- if event_names|select("defined")|list|length %}
        or event_name in ('{{ event_names|select("defined")|join("','") }}') --filter on event_name if provided
    {%- endif %}
    {%- if event_names|select("undefined")|list|length %}
        or event_name is null
    {% endif %}
  )
  {%- else -%}
    (1=1)
  {%- endif -%}
{% endmacro %}
