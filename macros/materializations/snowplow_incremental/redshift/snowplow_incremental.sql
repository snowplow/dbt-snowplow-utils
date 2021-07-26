
{% materialization snowplow_incremental, default -%}

  {% set unique_key = config.get('unique_key') %}
  {% set upsert_date_key = config.get('upsert_date_key') %}
  {% set disable_upsert_lookback = config.get('disable_upsert_lookback') %}
  {% set full_refresh_mode = flags.FULL_REFRESH %}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or full_refresh_mode %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}

      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql = snowplow_utils.snowplow_incremental_upsert(tmp_relation,
                                                     target_relation,
                                                     unique_key=unique_key,
                                                     upsert_date_key=upsert_date_key,
                                                     disable_upsert_lookback=disable_upsert_lookback) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}


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
