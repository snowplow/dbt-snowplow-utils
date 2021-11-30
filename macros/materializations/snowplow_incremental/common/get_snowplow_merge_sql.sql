{% macro get_snowplow_merge_sql(target, source, unique_key, dest_columns, predicates, include_sql_header) -%}
  {{ adapter.dispatch('get_snowplow_merge_sql', 'snowplow_utils')(target, source, unique_key, dest_columns, predicates, include_sql_header) }}
{%- endmacro %}

{% macro default__get_snowplow_merge_sql(target, source, unique_key, dest_columns, predicates, include_sql_header) -%}
  {%- set predicates = [] if predicates is none else [] + predicates -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {% if unique_key %}
    {% set unique_key_match %}
        DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    {% endset %}
  {% else %}
    {% set unique_key_match %}
        false
    {% endset %}
  {% endif %}

  {{ sql_header if sql_header is not none and include_sql_header }}

  merge into {{ target }} as DBT_INTERNAL_DEST
  using {{ source }} as DBT_INTERNAL_SOURCE
  on {{ unique_key_match }}
  {% if predicates %} and {{ predicates | join(' and ') }} {% endif %}

  {% if unique_key %}
  when matched then update set
    {% for column_name in update_columns -%}
      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
      {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
  {% endif %}

  when not matched then insert
    ({{ dest_cols_csv }})
  values
    ({{ dest_cols_csv }})

{% endmacro %}
