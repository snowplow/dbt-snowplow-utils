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

-- page view context is given as json string in csv. Parse json
with prep as (
  select
    * EXCLUDE(contexts_com_snowplowanalytics_user_identifier_1_0_0, contexts_com_snowplowanalytics_user_identifier_2_0_0,
contexts_com_snowplowanalytics_session_identifier_1_0_0, contexts_com_snowplowanalytics_session_identifier_2_0_0,
contexts_com_snowplowanalytics_custom_entity_1_0_0),
    json(contexts_com_snowplowanalytics_snowplow_web_page_1_0_0) as contexts_com_snowplowanalytics_snowplow_web_page_1,
    json(unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
    json(unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1_0_0) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
    json(contexts_com_iab_snowplow_spiders_and_robots_1_0_0) as contexts_com_iab_snowplow_spiders_and_robots_1,
    json(contexts_com_snowplowanalytics_snowplow_ua_parser_context_1_0_0) as contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
    json(contexts_nl_basjes_yauaa_context_1_0_0) as contexts_nl_basjes_yauaa_context_1,
    json(contexts_com_snowplowanalytics_user_identifier_1_0_0) as contexts_com_snowplowanalytics_user_identifier_1,
    json(contexts_com_snowplowanalytics_user_identifier_2_0_0) as contexts_com_snowplowanalytics_user_identifier_2,
    json(contexts_com_snowplowanalytics_session_identifier_1_0_0) as contexts_com_snowplowanalytics_session_identifier_1,
    json(contexts_com_snowplowanalytics_session_identifier_2_0_0) as contexts_com_snowplowanalytics_session_identifier_2,
    json(contexts_com_snowplowanalytics_custom_entity_1_0_0) as contexts_com_snowplowanalytics_custom_entity_1
  from {{ ref('snowplow_events') }}
  )

, flatten as (
  select
    *,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'basis_for_processing' as basisForProcessing,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'consent_scopes' as consentScopes,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'consent_url' as consentUrl,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'consent_version' as consentVersion,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'domains_applied' as domainsApplied,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'event_type' as eventType,
    unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1->0->>'gdpr_applies' as gdprApplies,
    unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1->0->>'elapsed_time' as elapsedTime

  from prep

)

select
  app_id,
  platform,
  etl_tstamp,
  collector_tstamp,
  dvce_created_tstamp,
  event,
  event_id,
  txn_id,
  name_tracker,
  v_tracker,
  v_collector,
  v_etl,
  user_id,
  user_ipaddress,
  user_fingerprint,
  domain_userid,
  domain_sessionidx,
  network_userid,
  geo_country,
  geo_region,
  geo_city,
  geo_zipcode,
  geo_latitude,
  geo_longitude,
  geo_region_name,
  ip_isp,
  ip_organization,
  ip_domain,
  ip_netspeed,
  page_url,
  page_title,
  page_referrer,
  page_urlscheme,
  page_urlhost,
  page_urlport,
  page_urlpath,
  page_urlquery,
  page_urlfragment,
  refr_urlscheme,
  refr_urlhost,
  refr_urlport,
  refr_urlpath,
  refr_urlquery,
  refr_urlfragment,
  refr_medium,
  refr_source,
  refr_term,
  mkt_medium,
  mkt_source,
  mkt_term,
  mkt_content,
  mkt_campaign,
  se_category,
  se_action,
  se_label,
  se_property,
  se_value,
  tr_orderid,
  tr_affiliation,
  tr_total,
  tr_tax,
  tr_shipping,
  tr_city,
  tr_state,
  tr_country,
  ti_orderid,
  ti_sku,
  ti_name,
  ti_category,
  ti_price,
  ti_quantity,
  pp_xoffset_min,
  pp_xoffset_max,
  pp_yoffset_min,
  pp_yoffset_max,
  useragent,
  br_name,
  br_family,
  br_version,
  br_type,
  br_renderengine,
  br_lang,
  br_features_pdf,
  br_features_flash,
  br_features_java,
  br_features_director,
  br_features_quicktime,
  br_features_realplayer,
  br_features_windowsmedia,
  br_features_gears,
  br_features_silverlight,
  br_cookies,
  br_colordepth,
  br_viewwidth,
  br_viewheight,
  os_name,
  os_family,
  os_manufacturer,
  os_timezone,
  dvce_type,
  dvce_ismobile,
  dvce_screenwidth,
  dvce_screenheight,
  doc_charset,
  doc_width,
  doc_height,
  tr_currency,
  tr_total_base,
  tr_tax_base,
  tr_shipping_base,
  ti_currency,
  ti_price_base,
  base_currency,
  geo_timezone,
  mkt_clickid,
  mkt_network,
  etl_tags,
  dvce_sent_tstamp,
  refr_domain_userid,
  refr_dvce_tstamp,
  domain_sessionid,
  derived_tstamp,
  event_vendor,
  event_name,
  event_format,
  event_version,
  event_fingerprint,
  true_tstamp,
  load_tstamp,
  contexts_com_snowplowanalytics_snowplow_web_page_1,
  struct_pack(
    basisForProcessing := basisForProcessing,
    consentScopes := consentScopes,
    consentUrl := consentUrl,
    consentVersion := consentVersion,
    domainsApplied := domainsApplied,
    eventType := eventType,
    gdprApplies := gdprApplies
  ) as unstruct_event_com_snowplowanalytics_snowplow_consent_preferences_1,
  struct_pack(
    elapsedTime := elapsedTime
  ) as unstruct_event_com_snowplowanalytics_snowplow_cmp_visible_1,
  contexts_com_iab_snowplow_spiders_and_robots_1,
  contexts_com_snowplowanalytics_snowplow_ua_parser_context_1,
  contexts_nl_basjes_yauaa_context_1,
  CASE WHEN json_extract_string(contexts_com_snowplowanalytics_user_identifier_1[0], '$.user_id') IS NOT NULL 
       THEN [struct_pack(userId := json_extract_string(contexts_com_snowplowanalytics_user_identifier_1[0], '$.user_id'))]
       ELSE NULL END as contexts_com_snowplowanalytics_user_identifier_1,
  CASE WHEN json_extract_string(contexts_com_snowplowanalytics_user_identifier_2[0], '$.user_id') IS NOT NULL
       THEN [struct_pack(userId := json_extract_string(contexts_com_snowplowanalytics_user_identifier_2[0], '$.user_id'))]
       ELSE NULL END as contexts_com_snowplowanalytics_user_identifier_2,
  CASE WHEN json_extract_string(contexts_com_snowplowanalytics_session_identifier_1[0], '$.session_id') IS NOT NULL
       THEN [struct_pack(sessionId := json_extract_string(contexts_com_snowplowanalytics_session_identifier_1[0], '$.session_id'))]
       ELSE NULL END as contexts_com_snowplowanalytics_session_identifier_1,
  CASE WHEN json_extract_string(contexts_com_snowplowanalytics_session_identifier_2[0], '$.session_identifier') IS NOT NULL
       THEN [struct_pack(sessionIdentifier := json_extract_string(contexts_com_snowplowanalytics_session_identifier_2[0], '$.session_identifier'))]
       ELSE NULL END as contexts_com_snowplowanalytics_session_identifier_2,
  {% if var("snowplow__custom_test", false) %}
        contexts_com_snowplowanalytics_custom_entity_1
  {% else %}
        struct_pack(contents := NULL) as contexts_com_snowplowanalytics_custom_entity_1
  {% endif %}

from flatten
