{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}

{% set tests_yml %}
tests:
  - paths: ['context_1.a', 'context_0.a']
    field_alias: 'a'
    include_field_alias: True
    relation_alias: 'b'
    expected: 'coalesce(b.context_1.a, b.context_0.a) as a'
  - paths: ['context_1.a']
    field_alias: 'a'
    include_field_alias: false
    relation_alias: Null
    expected: 'coalesce(context_1.a)'
{% endset %}

{% set tests = fromyaml(tests_yml)['tests'] %}

with prep as (
{% for test in tests %}

  {%- set actual = snowplow_utils.coalesce_field_paths(
                                              paths=test.paths,
                                              field_alias=test.field_alias,
                                              include_field_alias=test.include_field_alias,
                                              relation_alias=test.relation_alias
                                                    )-%}
  {%- set expected = test.expected -%}
  select "{{ actual}}" as actual, "{{ expected }}" as expected
  {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select *
from prep
where actual != expected
