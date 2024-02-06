{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro app_id_filter(app_ids) %}
  {%- if app_ids|length -%}
  ( 
    1=0
    {%- if app_ids|select("defined")|list|length %}
        or app_id in ('{{ app_ids|select("defined")|join("','") }}') --filter on app_id if provided
    {%- endif %}
    {%- if app_ids|select("undefined")|list|length %}
        or app_id is null
    {% endif %}
  )
  {%- else -%}
    (1=1)
  {%- endif -%}
{% endmacro %}
