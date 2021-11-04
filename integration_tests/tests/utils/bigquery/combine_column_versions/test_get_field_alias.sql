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
