
{% macro snowplow_incremental_upsert(tmp_relation, target_relation, unique_key=none, upsert_date_key=none, disable_upsert_lookback=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}

    {%- if unique_key is not none and upsert_date_key is not none -%}
    delete
    from {{ target_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ tmp_relation }}
    )
    and {{ upsert_date_key }} >=

    {%- if disable_upsert_lookback -%} 
      (select min({{ upsert_date_key }}) as lower_limit from {{ tmp_relation }} );
    {%- else -%}
      (select 
        {{ dbt_utils.dateadd(datepart='day',
                             interval= -var("snowplow__upsert_lookback_days", 30),
                             from_date_or_timestamp="min("+upsert_date_key+")") }} as lower_limit
       from {{ tmp_relation }} );
    {%- endif %}

    {%- endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    );
{%- endmacro %}


{# Creating snowplow is_incremental() to include snowplow_incremental materilization #}
{% macro snowplow_is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {% if not execute %}
    {{ return(False) }}
  {% else %}
    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {{ return(relation is not none
              and relation.type == 'table'
              and model.config.materialized in ['incremental','snowplow_incremental']
              and not should_full_refresh()) }}
  {% endif %}
{% endmacro %}
