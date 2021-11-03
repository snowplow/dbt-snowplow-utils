{{ config(pre_hook="{{ snowplow_utils.snowplow_delete_from_manifest(
														models=var('models_to_delete',[]),
														incremental_manifest_table=ref('data_snowplow_delete_from_manifest_staging')) }}",
					tags=["requires_script"]) }}

-- data_snowplow_delete_from_manifest_staging is manifest table to delete from.
-- data_snowplow_delete_from_manifest is the manifest table to select from to get the expected results
-- Note: Test covers functionality however when running the macro on-run-start hook, transaction behaviour changes.
-- Wrapped delete statement in transation so it commits. BQ wouldnt just support 'commit;' without opening trans. Snowflake behaviour untested.

select *

from {{ ref('data_snowplow_delete_from_manifest_staging') }}

