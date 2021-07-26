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
