{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{#
Seeds a pre-existing duplicate session_identifier row directly into
test_lifecycle_manifest_dedupe_actual, between its full-refresh run and its incremental run, to
simulate a manifest already corrupted by session_identifier reuse (AISP-1714). Duplicates whatever
row is already there rather than hardcoding values, since the model's own event fixtures are the
source of truth for what that row looks like.
#}
{% macro test_duplicate_lifecycle_manifest_session_identifier() %}

    {% set relation = ref('test_lifecycle_manifest_dedupe_actual') %}

    {% set column_list %}
        session_identifier, user_identifier, start_tstamp, end_tstamp
        {%- if target.type in ['databricks', 'spark'] -%}
        , start_tstamp_date
        {%- endif -%}
    {% endset %}

    {% set duplicate_query %}
        insert into {{ relation }} ({{ column_list }})
        select {{ column_list }}
        from {{ relation }}
    {% endset %}

    {% do run_query(duplicate_query) %}

    {% do log("Snowplow: seeded a duplicate session_identifier row into " ~ relation ~ " to simulate a pre-existing manifest duplicate (AISP-1714)", info=true) %}

{% endmacro %}
