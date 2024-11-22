{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

{% macro base_create_snowplow_events_this_run_t(run_limits_table, app_ids, snowplow_events_database=none, snowplow_events_schema='atomic', snowplow_events_table='events', event_names=none, custom_filter=none) %}
    {{ return(adapter.dispatch('base_create_snowplow_events_this_run_t', 'snowplow_utils')(run_limits_table, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, event_names, custom_filter)) }}
{% endmacro %}

{% macro default__base_create_snowplow_events_this_run_t(run_limits_table, app_ids, snowplow_events_database, snowplow_events_schema, snowplow_events_table, event_names, custom_filter) %}
    {%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref(run_limits_table),
                                                                            'lower_limit',
                                                                            'upper_limit') %}
    {% set snowplow_events = api.Relation.create(database=snowplow_events_database, schema=snowplow_events_schema, identifier=snowplow_events_table) %}

    {% set events_this_run_query %}
        with new_events AS (
    
            select *

            from {{ snowplow_events }}
            
            where load_tstamp > {{ lower_limit }} and load_tstamp < {{ upper_limit }}
            
            and {{ snowplow_utils.app_id_filter(app_ids) }}
            
            and {{ snowplow_utils.event_name_filter(event_names) }}
            
            {% if custom_filter is not none %}
                {% set disallowed = [';', '--', '/*', '*/', 'drop', 'delete', 'alter', 'insert', 'truncate'] %}
                {% for item in disallowed %}
                    {% if item in custom_filter | lower %}
                        {{ exceptions.raise_compiler_error("Unsafe SQL detected in custom_filter: contains '" ~ item ~ "'") }}
                    {% endif %}
                {% endfor %}
                and {{ custom_filter }}
            {% endif %}
        )

        select
        *
        from new_events a

        qualify row_number() over (partition by a.event_id order by a.load_tstamp, a.dvce_created_tstamp) = 1
    {% endset %}
    
    {{ return(events_this_run_query) }}

{% endmacro %}
