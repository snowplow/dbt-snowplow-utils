{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}

{% set tests_yml %}
tests:
  - fields_by_col_version:
    - - field_name: a
        path: context_1.a
        nested_level: 1
    - - field_name: a
        path: context_0.a
        nested_level: 1
      - field_name: d.e
        path: context_0.d.e
        nested_level: 2
    expected:
    - field_name: a
      field_paths:
      - context_1.a
      - context_0.a
      nested_level: 1
    - field_name: d.e
      field_paths:
      - context_0.d.e
      nested_level: 2
{% endset %}

{% set tests = fromyaml(tests_yml)['tests'] %}

with prep as (
{% for test in tests %}

  {%- set actual = snowplow_utils.merge_fields_across_col_versions(
                                                    fields_by_col_version=test.fields_by_col_version
                                                    )-%}
  {%- set expected = test.expected -%}
  select "{{ actual}}" as actual, "{{ expected }}" as expected
  {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select *
from prep
where actual != expected
