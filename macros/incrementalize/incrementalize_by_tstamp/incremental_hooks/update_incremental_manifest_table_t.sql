{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{# Updates the incremental manifest table at the run end with the latest tstamp consumed per model #}
{% macro update_incremental_manifest_table_t(manifest_table, base_events_table, models) -%}

  {{ return(adapter.dispatch('update_incremental_manifest_table_t', 'snowplow_utils')(manifest_table, base_events_table, models)) }}

{% endmacro %}

{% macro default__update_incremental_manifest_table_t(manifest_table, base_events_table, models) -%}

  {% if models %}

    {% set last_success_query %}
      select
        b.model,
        a.last_processed_load_tstamp,
        a.first_processed_load_tstamp

      from
        (select max(load_tstamp) as last_processed_load_tstamp,
                min(load_tstamp) as first_processed_load_tstamp from {{ base_events_table }}) a,
        ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %}) b

      where a.last_processed_load_tstamp is not null -- if run contains no data don't add to manifest
    {% endset %}

    merge into {{ manifest_table }} m
    using ( {{ last_success_query }} ) s
    on m.model = s.model
    when matched then
        update set last_processed_load_tstamp = greatest(m.last_processed_load_tstamp, s.last_processed_load_tstamp),
                    first_processed_load_tstamp = coalesce(m.first_processed_load_tstamp, s.first_processed_load_tstamp)
        
    when not matched then
          insert (model, last_processed_load_tstamp, first_processed_load_tstamp)
          values (s.model, s.last_processed_load_tstamp, s.first_processed_load_tstamp);

    {% if target.type == 'snowflake' %}
      commit;
    {% endif %}

  {% endif %}

{%- endmacro %}

