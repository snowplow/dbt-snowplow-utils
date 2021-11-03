{{ config(enabled=(target.type == 'bigquery' | as_bool()) )}}

{# TODO: Add test for compiler error when column is not of dtype record
         Improve test by query table containing records to validate paths actually execute #}

{% set columns = [{"name":"col_a", "dtype":"RECORD", "mode":"NULLABLE"}, {"name":"col_b", "dtype":"RECORD", "mode":"REPEATED"}] %}
{% set field = {"name": "field_a"} %}
{% set expected_field_paths = ['col_a.field_a', 'col_b[safe_offset(0)].field_a'] %}

with prep as (
{% for col in columns %}
  {%- set actual_field_path = snowplow_utils.get_record_field_path(column=col, field=field) -%}
  {%- set expected_field_path = expected_field_paths[loop.index0] -%}
  select '{{ actual_field_path}}' as actual_field_path, '{{ expected_field_path }}' as expected_field_path
  {% if not loop.last %} union all {% endif %}
{% endfor %}
)

select * 
from prep
where actual_field_path != expected_field_path

