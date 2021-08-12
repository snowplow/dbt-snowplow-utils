[![early-release]][tracker-classificiation] [![License][license-image]][license] [![Discourse posts][discourse-image]][discourse]

![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)
# snowplow-utils

To be used in conjunction with the [snowplow-web package][snowplow-web].

Includes:
- Custom incremental materialization, 'snowplow_incremental'.
- Pre and post hooks to handle incremental processing of events.
- Various helper macros used throughout the snowplow-web package.

## Macros
There are many macros contained in this package, with the majority designed for use internally at Snowplow. There are however a selection that were intended for public use and that can assist you in modelling Snowplow data. The documentation for these macros can be found below.

### get_columns_in_relation_by_column_prefix ([source](macros/bigquery/combine_column_versions.sql))
This macro returns an array of column names within a relation that start with the given column prefix. This is useful when you have multiple versions of a column within a table and want to dynamically identify all versions.

Arguments:
 - `relation`: The relation from which to search for matching columns.
 - `column_prefix`: The column prefix to search for.

Usage:
```sql
{% set matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(
                          relation=ref('snowplow_web_base_events_this_run'),
                          column_prefix='custom_context_1_0_'
                          ) %}

{{ matched_columns }}

# Renders to something like:
['custom_context_1_0_1','custom_context_1_0_2','custom_context_1_0_3']
```
The order of the matched columns is donated by their ordinal position. 

### combine_column_versions ([source](macros/bigquery/combine_column_versions.sql))
*BigQuery Only.* This macro is designed for primarily for combining versions of custom context or an unstructured event column from the Snowplow events table in BigQuery. 

As your schemas for such columns evolve, multiple versions of the same column will be created in your events table e.g. `custom_context_1_0_0`, `custom_context_1_0_1`. These columns contain nested fields i.e. are of a datatype `RECORD`. When modeling Snowplow data it can be useful to combine or coalesce each nested field across all versions of the column for a continuous view over time.

Note: 
- Only first level fields are un-nested and combined by this macro i.e. any nested `RECORD` columns will not be unpacked.
- If the `RECORD` column is of the mode `REPEATED`, only the first element in the array will be used.

Arguments:
- `relation`: The relation from which to search for matching columns.
- `column_prefix`: The column prefix to search for.
- `source_fields`: Optional. The subset of fields within the column to return.
- `renamed_fields`: Optional. An array of names to rename the `source_fields` to. If you pass this arg you must also pass the `source_fields` arg. They must be of the same length and order.
- `relation_alias`: Optional. The relation's assigned alias. If passed, this will be appended to the full path for each field. For example, if `relation_alias=a`, the full path would be `a.col_1.field_1`. This can be useful when you have multiple relations within your model with the same columns. 

Returns:
- An array, with each item being a string of coalesced paths to a field across each version of the column. The order of the coalesce is determined by the version of the column, with the latest taking precedent.

Usage:
```sql
{%- set combined_fields = snowplow_utils.combine_column_versions(
                                relation=ref('snowplow_web_base_events_this_run'),
                                column_prefix='custom_context_1_',
                                source_fields=['field_a', 'field_b']
                                ) -%}

select
{% for field in combined_fields %}
  {{field}} {%- if not loop.last %},{% endif %}
{% endfor %}

from {{ ref('snowplow_web_base_events_this_run') }}

# Renders to something like:
select
  coalesce(custom_context_1_0_1[safe_offset(0)].field_a, custom_context_1_0_0[safe_offset(0)].field_a) as field_a,
  coalesce(custom_context_1_0_1[safe_offset(0)].field_b, custom_context_1_0_0[safe_offset(0)].field_b) as field_b

from {{ ref('snowplow_web_base_events_this_run') }}

```

### is_run_with_new_events ([source](macros/snowplow_utils.sql))
This macro is designed for use with Snowplow data modelling packages like snowplow-web. It can be used in any incremental models, to effectively block the incremental model from being updated with old data which it has already consumed. This saves cost as well as preventing historical data from being overwritten with partially complete data (due to a batch back-fill for instance). The macro utilizes the `snowplow_web_incremental_manifest` table to determine whether the model from which the macro is called, i.e. `{{ this }}`, has already consumed the data in the given run. If it has, it returns `false`. If the data in the run contains new data, `true` is returned.

Arguments:
- `package_name`: The modeling package name i.e. `snowplow-web` (`snowplow-mobile` to follow).

Returns:
- Boolean. `true` if the run contains new events previously not consumed by `this`, `false` otherwise.

Usage:
```sql
{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='page_view_id',
    upsert_date_key='start_tstamp'
  ) 
}}

select
	...

from {{ ref('snowplow_web_base_events_this_run' ) }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_web') }} --returns false if run doesn't contain new events.
```

### snowplow_delete_from_manifest ([source](macros/snowplow_utils.sql))
The snowplow-web package makes use of a centralised manifest system to record the current state of the package. There may be times when you want to remove the metadata associated with particular models from the manifest, for instance to replay events through a particular model. This can be performed as part of the run-start operation of the snowplow-web package, as described in the [docs][snowplow-web-docs]. You can however perform this operation independently using the `snowplow_delete_from_manifest` macro.

Arguments:
- `package_name`: The modeling package name i.e. `snowplow-web` (`snowplow-mobile` to follow).
- `models`: Either an array of models to delete, or a string for a single model.

Usage:
```bash
dbt run-operation snowplow_delete_from_manifest --args "{package_name: snowplow_web, models: ['snowplow_web_page_views','snowplow_web_sessions']}"
# or
dbt run-operation snowplow_delete_from_manifest --args "{package_name: snowplow_web, models: snowplow_web_page_views}"
```

### snowplow_teardown_all ([source](macros/snowplow_utils.sql))
This macro will drop all the manifest tables and run limit tables used by the snowplow-web package. These include:
- `snowplow_manifest.snowplow_web_incremental_manifest`
- `snowplow_manifest.snowplow_web_current_incremental_tstamp`
- `snowplow_manifest.snowplow_web_base_sessions_lifecycle_manifest`

This macro is optionally executed as part of the run-start operation of the snowplow-web package, as described in the [docs][snowplow-web-docs]. You can however perform this operation independently using the `snowplow_teardown_all` macro.

**Note: Use with caution. The information in these manifests is critical for the snowplow-web package to operate**

Arguments:
- `package_name`: The modeling package name i.e. `snowplow-web` (`snowplow-mobile` to follow).

Usage:
```bash
dbt run-operation snowplow_teardown_all --args "{package_name: snowplow_web}"
```

## Materializations
This package provides a custom incremental materialization, `snowplow_incremental`. This builds upon the out-of-the-box incremental materialization provided by dbt, by limiting the length of the table scans on the destination table. This improves both performance and reduces cost. The following methodology is used to calculate the limit of the table scan:

- The minimum date is found in the `tmp_relation`, based on the `upsert_date_key`
- By default, 30 days are subtracted from this date. This is set by `snowplow__upsert_lookback_days`. We found when modeling Snowplow data, having this look-back period of 30 days can help minimise the chance of introducing duplicates in your destination table. Reducing the number of look-back days will improve performance further but increase the risk of duplicates.
- The look-back can be disabled altogether, by setting `disable_upsert_lookback=true` in your model's config (see below). This is not recommended for most use cases.

As is the case with the native dbt incremental materialization, the strategy varies between adapters.

### Redshift ([source](macros/materializations/snowplow_incremental/redshift/snowplow_incremental.sql))
Like the native materialization, the `snowplow_incremental` materialization strategy is delete and insert however a limit has been imposed on how far to scan the destination table in order to improve performance:

```sql
delete
from {{ destination_table }}
where {{ unique_key }} in (select {{ unique_key }} from {{ tmp_relation }})
and {{ upsert_date_key }} >= (select dateadd('day', -{{ snowplow__upsert_lookback_days }}, min({{ upsert_date_key }})) as lower_limit from {{ tmp_relation }});

insert into {{ destination_table }}
(select * from {{ tmp_relation }});
```

This materialization can be implemented as follows:
```sql
{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='page_veiw_id', # The primary key of your model
    upsert_date_key='start_tstamp', # The date key to be used for the look back
    disable_upsert_lookback=true # Optional. Will disable the look-back period during the upsert to the destination table.
  ) 
}}
```

### BigQuery ([source](macros/materializations/snowplow_incremental/bigquery/snowplow_incremental.sql))
Like the native materialization, the `snowplow_incremental` materialization strategy is [merge](https://docs.getdbt.com/reference/resource-configs/bigquery-configs#the-merge-strategy) however limits are calculated to allow for partition pruning on the destination table saving cost:

```sql
/*
  Create a temporary table from the model SQL
*/
create temporary table {{ model_name }}__dbt_tmp as (
  {{ model_sql }}
);

/*
  Find merge limits
*/

declare dbt_partition_lower_limit, dbt_partition_upper_limit date;
set (dbt_partition_lower_limit, dbt_partition_upper_limit) = (
    select as struct
         dateadd('day', -{{ snowplow__upsert_lookback_days }}, min({{ partition_by_key }})) as dbt_partition_lower_limit,
         max({{ partition_by_key }}) as dbt_partition_upper_limit
    from {{ model_name }}__dbt_tmp
);

/*
  Update or insert into destination table. Limit the table scan on the destination table.
*/
merge into {{ destination_table }} DEST
using {{ model_name }}__dbt_tmp SRC
on SRC.{{ unique_key }} = DEST.{{ unique_key }}
and DEST.{{ partition_by_key }} between dbt_partition_lower_limit and dbt_partition_upper_limit -- Prune partitions on DEST

when matched then update ...

when not matched then insert ...
```

This materialization can be implemented as follows:
```sql
{{ 
  config(
    materialized='snowplow_incremental',
    unique_key='page_view_id', # The primary key of your model
    partition_by = {
      "field": "start_tstamp",
      "data_type": "timestamp",
      "granularity": "day"
    }, # Adds partitions to destination table. This field is also used to determine the upsert limits dbt_partition_lower_limit, dbt_partition_upper_limit
    disable_upsert_lookback=true # Optional. Will disable the look-back period during the upsert to the destination table. 
  ) 
}}
```
**Note you must provide the `partition_by` clause to use this materialization. All `data_types` are supported except `int64`.**

### Notes 
- If using this the `snowplow_incremental` materialization, **the native dbt `is_incremental()` macro will not recognize the model as incremental**. Please use the `snowplow_utils.snowplow_is_incremental()` macro instead, which operates in the same way.
- `snowplow__upsert_lookback_days` defaults to 30 days. If you set `snowplow__upsert_lookback_days` to too short a period, duplicates can occur in your incremental table. 

# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-utils package is Copyright 2021 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[tracker-classificiation]: https://docs.snowplowanalytics.com/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC


[discourse-image]: https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscourse.snowplowanalytics.com%2F
[discourse]: http://discourse.snowplowanalytics.com/
[snowplow-web]: https://github.com/snowplow/dbt-snowplow-web
[snowplow-web-docs]: https://snowplow.github.io/dbt-snowplow-web/#!/overview/snowplow_web
