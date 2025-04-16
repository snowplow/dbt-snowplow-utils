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
        a.last_success,
        a.first_success

      from
        (select max(load_tstamp) as last_success,
                min(load_tstamp) as first_success from {{ base_events_table }}) a,
        ({% for model in models %} select '{{model}}' as model {%- if not loop.last %} union all {% endif %} {% endfor %}) b

      where a.last_success is not null -- if run contains no data don't add to manifest
    {% endset %}

    merge into {{ manifest_table }} m
    using ( {{ last_success_query }} ) s
    on m.model = s.model
    when matched then
        update set last_success = greatest(m.last_success, s.last_success),
                    first_success = coalesce(m.first_success, s.first_success)
        
    when not matched then
          insert (model, last_success, first_success)
          values (s.model, s.last_success, s.first_success);

    {% if target.type == 'snowflake' %}
      commit;
    {% endif %}

  {% endif %}

{%- endmacro %}

