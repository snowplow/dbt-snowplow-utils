{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}

{% set tests = [
  {'field': 'a.b', 'expected': ('a.b', 'a_b')},
  {'field': ('a', 'A'), 'expected': ('a', 'A')}
] %}

with prep as (
{% for test in tests %}

  {%- set actual = snowplow_utils.get_field_alias(field=test.field)-%}
  {%- set expected = test.expected -%}

  select "{{ actual}}" as actual, "{{ expected }}" as expected
  {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select *
from prep
where actual != expected
