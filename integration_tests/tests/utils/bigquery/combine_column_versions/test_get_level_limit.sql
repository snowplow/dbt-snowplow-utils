{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}

{% set tests_yml %}
tests:
  - level: 3
    level_filter: 'equalto'
    required_field_names: []
    expected: 3
  - level: 3
    level_filter: 'lessthan'
    required_field_names: []
    expected: 2
  - level: 3
    level_filter: 'greaterthan'
    required_field_names: []
    expected: null
  - level: null
    level_filter: 'equalto'
    required_field_names: []
    expected: null
  - level: null
    level_filter: 'equalto'
    required_field_names: ['a','c.d']
    expected: 2
  - level: 3
    level_filter: 'dummy'
    required_field_names: []
    expected: 'Error: Incompatible level filter arg. Accepted args: equalto, lessthan, greaterthan'
  - level: 1
    level_filter: 'equalto'
    required_field_names: ['a']
    expected: 'Error: Cannot filter fields by both `required_fields` and `level` arg. Please use only one.'
  - level: null
    level_filter: 'lessthan'
    required_field_names: ['a','c.d']
    expected: 'Error: To filter fields using `required_fields` arg, `level_filter` must be set to `equalto`'
{% endset %}

{% set tests = fromyaml(tests_yml)['tests'] %}

with prep as (
{% for test in tests %}

  {%- set actual = snowplow_utils.get_level_limit(
                                              level=test.level,
                                              level_filter=test.level_filter,
                                              required_field_names=test.required_field_names
                                              ) -%}
  {%- set expected = test.expected -%}
  select '{{ actual}}' as actual, '{{ expected }}' as expected
  {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * 
from prep
where actual != expected
