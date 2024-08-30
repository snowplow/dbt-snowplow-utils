{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{
    config(
        tags=['base_macro']
    )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref(var('snowplow__base_sessions', 'snowplow_base_sessions_this_run_actual')),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

{% set snowplow_session_identifiers = var("snowplow__session_identifiers") %}
{% set snowplow_entities_or_sdes = var("snowplow__entities_or_sdes") %}
{% set snowplow_custom_sql = 'cast(null as ' ~ dbt.type_string() ~ ') as custom_contents' %}
{% set snowplow_session_sql = '' %}

{% if var('snowplow__custom_test', false) %}
    {% set snowplow_session_identifiers = snowplow_utils.get_value_by_target_type(bigquery_val=var("snowplow__bigquery_session_identifiers"), 
                                                                                  snowflake_val=var("snowplow__snowflake_session_identifiers"),
                                                                                  databricks_val=var("snowplow__databricks_session_identifiers"),
                                                                                  spark_val=var("snowplow__spark_session_identifiers"),
                                                                                  postgres_val=var("snowplow__postgres_session_identifiers"),
                                                                                  redshift_val=var("snowplow__postgres_session_identifiers"))%}
    {% set snowplow_entities_or_sdes = var("snowplow__custom_entities_or_sdes") %}
    {% set snowplow_custom_sql = snowplow_utils.get_value_by_target_type(bigquery_val=var("snowplow__bigquery_custom_sql"),
                                                                        snowflake_val=var("snowplow__snowflake_custom_sql"), 
                                                                        databricks_val=var("snowplow__databricks_custom_sql"),
                                                                        spark_val=var("snowplow__spark_custom_sql")
                                                                        )%}
{% elif var('snowplow__session_test', false) %}
    {% set snowplow_session_sql = var("snowplow__custom_session_sql") %}
{% endif %}


{% set base_events_query = snowplow_utils.base_create_snowplow_events_this_run(
    sessions_this_run_table=var('snowplow__base_sessions', 'snowplow_base_sessions_this_run_actual'),
    session_identifiers=snowplow_session_identifiers,
    session_sql=snowplow_session_sql,
    session_timestamp=var('snowplow__session_timestamp', 'collector_tstamp'),
    derived_tstamp_partitioned=var('snowplow__derived_tstamp_partitioned', true),
    days_late_allowed=var('snowplow__days_late_allowed', 3),
    max_session_days=var('snowplow__max_session_days', 3),
    app_ids=var('snowplow__app_ids', []),
    snowplow_events_database=var('snowplow__database', target.database) if target.type not in ['databricks', 'spark'] else var('snowplow__databricks_catalog', 'hive_metastore') if target.type in ['databricks'] else var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_schema=var('snowplow__events_schema', 'snplw_utils_int_tests'),
    snowplow_events_table=var('snowplow__events_table', 'snowplow_events_stg'),
    entities_or_sdes=snowplow_entities_or_sdes,
    custom_sql=snowplow_custom_sql
) %}

with base_events AS (
    {{ base_events_query }}
)

select
    session_identifier
      -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres
    ,CASE WHEN event_id IN ('1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76', '9e983d4a-e07c-4858-8e97-bdb7feb31241', '17e6ae5e-d694-4241-8663-4118e950fc38') then 'true base'
        ELSE app_id end as app_id
    ,platform
    -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres
    ,CASE WHEN event_id IN ('17e6ae5e-d694-4241-8663-4118e950fc38', '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76') then timestamp '2021-03-01 20:58:12.682'
        WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' then timestamp '2021-03-03 21:27:37.134'
        ELSE etl_tstamp end as etl_tstamp
    ,CASE WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' then timestamp '2021-03-03 21:27:35.176'
        ELSE collector_tstamp end as collector_tstamp
    ,dvce_created_tstamp
    ,event_id
    ,txn_id
    ,name_tracker
    ,v_tracker
    ,v_collector
    ,v_etl
    ,user_id
    ,user_ipaddress
    ,user_fingerprint
    ,domain_userid
    ,domain_sessionidx
    ,CASE WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' THEN 'a3e0df02f9d1d13dc655e4102b0fd6fee52462ca808ade5d213c015f4a86a258'
        ELSE network_userid end as network_userid
    ,geo_country
    ,geo_region
    ,geo_city
    ,geo_zipcode
    ,geo_latitude
    ,geo_longitude
    ,geo_region_name
    ,ip_isp
    ,ip_organization
    ,ip_domain
    ,ip_netspeed
    ,page_url
    ,page_title
    ,page_referrer
    ,page_urlscheme
    ,page_urlhost
    ,page_urlport
    ,page_urlpath
    ,page_urlquery
    ,page_urlfragment
    ,refr_urlscheme
    ,refr_urlhost
    ,refr_urlport
    ,refr_urlpath
    ,refr_urlquery
    ,refr_urlfragment
    ,refr_medium
    ,refr_source
    ,refr_term
    ,mkt_medium
    ,mkt_source
    ,mkt_term
    ,mkt_content
    ,mkt_campaign
    ,se_category
    ,se_action
    ,se_label
    ,se_property
    ,se_value
    ,tr_orderid
    ,tr_affiliation
    ,tr_total
    ,tr_tax
    ,tr_shipping
    ,tr_city
    ,tr_state
    ,tr_country
    ,ti_orderid
    ,ti_sku
    ,ti_name
    ,ti_category
    ,ti_price
    ,ti_quantity
    ,pp_xoffset_min
    ,pp_xoffset_max
    ,pp_yoffset_min
    ,pp_yoffset_max
    ,useragent
    ,br_name
    ,br_family
    ,br_version
    ,br_type
    ,br_renderengine
    ,br_lang
    ,br_features_pdf
    ,br_features_flash
    ,br_features_java
    ,br_features_director
    ,br_features_quicktime
    ,br_features_realplayer
    ,br_features_windowsmedia
    ,br_features_gears
    ,br_features_silverlight
    ,br_cookies
    ,br_colordepth
    ,br_viewwidth
    ,br_viewheight
    ,os_name
    ,os_family
    ,os_manufacturer
    ,os_timezone
    ,dvce_type
    ,dvce_ismobile
    ,dvce_screenwidth
    ,dvce_screenheight
    ,doc_charset
    ,doc_width
    ,doc_height
    ,tr_currency
    ,tr_total_base
    ,tr_tax_base
    ,tr_shipping_base
    ,ti_currency
    ,ti_price_base
    ,base_currency
    ,geo_timezone
    ,mkt_clickid
    ,mkt_network
    ,etl_tags
    ,CASE WHEN event_id IN ('17e6ae5e-d694-4241-8663-4118e950fc38', '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76') then timestamp '2021-03-01 20:58:05.115'
        WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' THEN timestamp '2021-03-03 21:27:34.832'
        ELSE dvce_sent_tstamp end as dvce_sent_tstamp
    ,refr_domain_userid
    ,refr_dvce_tstamp
    ,domain_sessionid
    -- hard-coding due to non-deterministic outcome from row_number for Redshift/Postgres
    ,CASE WHEN event_id = '17e6ae5e-d694-4241-8663-4118e950fc38' then timestamp '2021-03-01 20:56:39.257'
        WHEN event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then timestamp '2021-03-01 20:56:39.192'
        WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' then timestamp '2021-03-03 21:27:32.345'
        ELSE derived_tstamp end as derived_tstamp
    ,event
    ,event_vendor
    ,event_name
    ,event_format
    ,event_version
    ,event_fingerprint
    ,true_tstamp
    ,load_tstamp
    ,user_identifier
    {% if var("snowplow__custom_test", false) %}
        ,CASE WHEN event_id = '9e983d4a-e07c-4858-8e97-bdb7feb31241' then 'a'
            WHEN event_id = '1b4b3b57-3cb7-4df2-a7fd-526afa9e3c76' then 'snowmen'
            WHEN event_id = '17e6ae5e-d694-4241-8663-4118e950fc38' then 'they'
            ELSE custom_contents end as custom_contents
    {% else %}
        ,cast(null as {{ type_string() }}) as custom_contents
    {% endif %}
    {% if target.type in ['redshift', 'postgres'] %}
    ,event_id_dedupe_index
    ,event_id_dedupe_count
    {% endif %}

from base_events
