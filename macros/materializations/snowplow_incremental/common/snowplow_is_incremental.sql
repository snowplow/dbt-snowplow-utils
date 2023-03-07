{# Creating snowplow is_incremental() to include snowplow_incremental materilization #}
{% macro snowplow_is_incremental() %}
  {#-- do not run introspective queries in parsing #}
  {% if not execute %}
    {{ return(False) }}
  {% else %}

    {%- set error_message = "Warning: the `snowplow_is_incremental` macro is deprecated as is the materialization, and should be replaced with dbt's `is_incremental` materialization. It will be removed completely in a future version of the package." -%}
    {%- do exceptions.warn(error_message) -%}

    {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
    {{ return(relation is not none
              and relation.type == 'table'
              and model.config.materialized in ['incremental','snowplow_incremental']
              and not should_full_refresh()) }}
  {% endif %}
{% endmacro %}
