{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{{ config(pre_hook="{{ snowplow_utils.snowplow_delete_from_manifest(
														models=var('models_to_delete',[]),
														incremental_manifest_table=ref('data_snowplow_delete_from_manifest_staging')) }}",
					tags=["requires_script"]) }}

-- data_snowplow_delete_from_manifest_staging is manifest table to delete from.
-- data_snowplow_delete_from_manifest is the manifest table to select from to get the expected results
-- Note: Test covers functionality however when running the macro on-run-start hook, transaction behaviour changes.
-- Wrapped delete statement in transation so it commits. BQ wouldnt just support 'commit;' without opening trans. Snowflake behaviour untested.

SELECT *
FROM {{ ref('data_snowplow_delete_from_manifest_staging') }}
