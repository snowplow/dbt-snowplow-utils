{% docs macro_app_id_filter %}
{% raw %}
Generates a `sql` filter for the values in `app_ids` applied on the `app_id` column.

#### Returns

`app_id in (...)` if any `app_ids` are provided, otherwise `true`.

#### Usage

```sql
app_id_filter(['web', 'mobile', 'news'])

-- returns
app_id in ('web', 'mobile', 'news')
```
{% endraw %}
{% enddocs %}

{% docs macro_get_columns_in_relation_by_column_prefix %}
{% raw %}
This macro returns an array of column objects within a relation that start with the given column prefix. This is useful when you have multiple versions of a column within a table and want to dynamically identify all versions.

#### Returns

An array of (column objects)[https://docs.getdbt.com/reference/dbt-classes#column]. The name of each column can be accessed with the name property.

#### Usage

```sql
get_columns_in_relation_by_column_prefix(ref('snowplow_web_base_events_this_run'), 'domain')

-- returns
['domain_sessionid', 'domain_userid', 'domain_sessionidx',...]

{% set matched_columns = snowplow_utils.get_columns_in_relation_by_column_prefix(
                    relation=ref('snowplow_web_base_events_this_run'),
                    column_prefix='custom_context_1_0_'
                    ) %}

{% for column in matched_columns %}
{{ column.name }}
{% endfor %}

# Renders to something like:
'custom_context_1_0_1'
'custom_context_1_0_2'
'custom_context_1_0_3'

```
{% endraw %}
{% enddocs %}

{% docs macro_get_schemas_by_pattern %}
{% raw %}
Given a pattern, finds and returns all schemas that match that pattern. Note that for databricks any single character matches (`_`) will not be properly translated due to databricks using a regex expression instead of a SQL `like` clause.

      #### Returns

      A list of schemas that match the pattern provided.

{% endraw %}
{% enddocs %}

{% docs macro_get_value_by_target_type %}
{% raw %}
Returns the value provided based on the `target.type`. This is useful when you need a different value based on which warehouse is being used e.g. cluster fields or partition keys.

#### Returns

The appropriate value for the target warehouse type, or an error if not an expected target type.

{% endraw %}
{% enddocs %}

{% docs macro_get_value_by_target %}
{% raw %}
This macro is designed to dynamically return values based on the target (`target.name`) you are running against. Your target names are defined in your [profiles.yml](https://docs.getdbt.com/reference/profiles.yml) file. This can be useful for dynamically changing variables within your project, depending on whether you are running in dev or prod.

#### Returns

The value relevant to the target environment

#### Usage

```yml

# dbt_project.yml
...
vars:
snowplow_web:
    snowplow__backfill_limit_days: "{{ snowplow_utils.get_value_by_target(dev_value=1, default_value=30, dev_target_name='dev') }}"

```
{% endraw %}
{% enddocs %}

{% docs macro_is_run_with_new_events %}
{% raw %}
This macro is designed for use with Snowplow data modelling packages like `snowplow-web`. It can be used in any incremental models, to effectively block the incremental model from being updated with old data which it has already consumed. This saves cost as well as preventing historical data from being overwritten with partially complete data (due to a batch back-fill for instance).

The macro utilizes the `snowplow_[platform]_incremental_manifest` table to determine whether the model from which the macro is called, i.e. `{{ this }}`, has already consumed the data in the given run. If it has, it returns `false`. If the data in the run contains new data, `true` is returned.

For the sessions lifecycle identifier it does not use the manifest as this table is not included in it.

#### Returns

`true` if the run contains new events previously not consumed by `this`, `false` otherwise.

#### Usage

```sql

{{
config(
    materialized='incremental',
    unique_key='screen_view_id',
    upsert_date_key='start_tstamp'
)
}}

select
...

from {{ ref('snowplow_mobile_base_events_this_run' ) }}
where {{ snowplow_utils.is_run_with_new_events('snowplow_mobile') }} --returns false if run doesn't contain new events.

```
{% endraw %}
{% enddocs %}

{% docs macro_log_message %}
{% raw %}
A wrapper macro for the `dbt_utils.pretty_log_format` using the `snowplow__has_log_enabled` to determine if the log is also printed to the stdout.
{% endraw %}
{% enddocs %}

{% docs macro_post_ci_cleanup %}
{% raw %}
This macro deletes all schemas that start with the specified `schema_pattern`, mostly for use before/after CI testing to ensure a clean start and removal of data after CI tests.
{% endraw %}
{% enddocs %}

{% docs macro_print_list %}
{% raw %}
Prints an array as a `seperator` separated quoted list.

#### Returns

Separated output of items in the list, quoted.
{% endraw %}
{% enddocs %}

{% docs macro_return_limits_from_model %}
{% raw %}
 Calculates and returns the minimum (lower) and maximum (upper) values of specified columns within the specified table. Useful to find ranges of a column within a table.

#### Returns

A list of two objects, the lower and upper values from the columns in the model
{% endraw %}
{% enddocs %}

{% docs macro_set_query_tag %}
{% raw %}
This macro takes a provided statement as argument and generates the SQL command to set this statement as the query_tag for Snowflake databases, and does nothing otherwise. It can be used to safely set the query_tag regardless of database type.

#### Returns

An alter session command set to the `query_tag` to the `statement` for Snowflake, otherwise nothing

#### Usage

```sql

{{ snowplow_utils.set_query_tag('snowplow_query_tag') }}

```
{% endraw %}
{% enddocs %}

{% docs macro_n_timedeltas_ago %}
{% raw %}
This macro takes the current timestamp and subtracts `n` units, as defined by the `timedelta_attribute`, from it. This is achieved using the Python datetime module, rather than querying your database. By combining this with the `get_value_by_target` macro, you can dynamically set dates depending on your environment.

#### Returns

Current timestamp minus `n` units.

#### Usage

```sql

{{ snowplow_utils.n_timedeltas_ago(1, 'weeks') }}

```

{% endraw %}
{% enddocs %}


{% docs macro_get_string_agg %}
{% raw %}

This macro takes care of harmonising cross-db `list_agg`, `string_agg` type functions. These are aggregate functions (i.e. to be used with a `group by`) that take values from grouped rows and concatenates them into a single string. This macro supports ordering values by an arbitrary column and ensuring unique values (i.e. applying distinct).

Note that databricks does not have list/string_agg function so a more complex expression is used.

#### Returns

The data warehouse appropriate sql to perform a list/string_agg. 

#### Usage

```sql
select
...
{{ snowplow_utils.get_string_agg('base_column', 'column_prefix', ';', 'order_by_col', sort_numeric=true, order_by_column_prefix='order_by_column_prefix', is_distict=True, order_desc=True)  }},
...
from ...
group by ...

```

{% endraw %}
{% enddocs %}


{% docs macro_get_split_to_array %}
{% raw %}

This macro takes care of harmonising cross-db split to array type functions. The macro supports a custom delimiter if your string is not delimited by a comma with no space (default).

#### Returns

The data warehouse appropriate sql to perform a split to array. 

#### Usage

```sql
select
...
{{ snowplow_utils.get_split_to_array('my_string_column', 'a', ', ') }}
...
from ... a

```

{% endraw %}
{% enddocs %}

{% docs macro_get_array_to_string %}
{% raw %}

This macro takes care of harmonising cross-db array to string type functions. The macro supports a custom delimiter if you don't want to use a comma with no space (default).

#### Returns

The data warehouse appropriate sql to convert an array to a string. 

#### Usage

```sql
select
...
{{ snowplow_utils.get_array_to_string('my_array_column', 'a', ', ') }}
...
from ... a

```

{% endraw %}
{% enddocs %}


{% docs macro_get_sde_or_context %}
{% raw %}

This macro exists for Redshift and Postgres users to more easily select their self-describing event and context tables and apply de-duplication before joining onto their (already de-duplicated) events table. The `root_id` and `root_tstamp` columns are by default returned as `schema_name_id` and `schema_name_tstamp` respectively, where `schema_name` is the value in the `schema_name` column of the table. In the case where multiple entities may be sent in the context (e.g. products in a search results), you should set the `single_entity` argument to `false` and use an additional criteria in your join (see [the snowplow docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-advanced-usage/dbt-duplicates/) for further details).

Note that is the responsibility of the user to ensure they have no duplicate names when using this macro multiple times or when a schema column name matches on already in the events table. In this case the `prefix` argument should be used and aliasing applied to the output.

#### Returns

CTE sql for deduplicating records from the schema table, without the schema details columns. The final CTE is the name of the original table.

#### Usage

With at most one entity per context:
```sql
with {{ snowplow_utils.get_sde_or_context('atomic', 'nl_basjes_yauaa_context_1', "'2023-01-01'", "'2023-02-01'")}}

select
...
from my_events_table a
left join nl_basjes_yauaa_context_1 b on 
    a.event_id = b.yauaa_context__id 
    and a.collector_tstamp = b.yauaa_context__tstamp
```
With the possibility of multiple entities per context, your events table must already be de-duped but still have a field with the number of duplicates:
```sql
with {{ snowplow_utils.get_sde_or_context('atomic', 'nl_basjes_yauaa_context_1', "'2023-01-01'", "'2023-02-01'", single_entity = false)}}

select
...,
count(*) over (partition by a.event_id) as duplicate_count
from my_events_table a
left join nl_basjes_yauaa_context_1 b on 
    a.event_id = b.yauaa_context__id 
    and a.collector_tstamp = b.yauaa_context__tstamp
    and mod(b.yauaa_context__index, a.duplicate_count) = 0
```

{% endraw %}
{% enddocs %}

{% docs macro_get_field %}
{% raw %}

This macro exists to make it easier to extract a field from our `unstruct_` and `contexts_` type columns for users in Snowflake, Databricks, and BigQuery (using a wildcard version number is only possible for BigQuery e.g. `column_name = 'contexts_nl_basjes_yauaa_context_1_*'`). The macro can handle type casting and selecting from arrays.

#### Returns

SQL snippet to select the field specified from the column

#### Usage

Extracting a single field
```sql

select
{{ snowplow_utils.get_field(column_name = 'contexts_nl_basjes_yauaa_context_1', 
                            field_name = 'agent_class', 
                            table_alias = 'a',
                            type = 'string',
                            array_index = 0)}} as yauaa_agent_class
from 
    my_events_table a

```

Extracting multiple fields
```sql

select
{% for field in [('field1', 'string'), ('field2', 'numeric'), ...] %}
  {{ snowplow_utils.get_field(column_name = 'contexts_nl_basjes_yauaa_context_1', 
                            field_name = field[0], 
                            table_alias = 'a',
                            type = field[1],
                            array_index = 0)}} as {{ field[0] }}
{% endfor %}

from 
    my_events_table a

```

{% endraw %}
{% enddocs %}
{% docs macro_parse_agg_dict %}
{% raw %}

This macro allows you to provide aggregations in a consistent and restricted way to avoid having to write the sql yourself. This is mostly for use within other packages to allow aggregations but not allow the user to add arbitrary SQL.

#### Returns

SQL snippet for the specified aggregation, aliased.

#### Usage

Extracting a single field
```sql

select
{{ snowplow_utils.parse_agg_dict({'type': 'countd', 'field': 'event_name', 'alias': 'distinct_event_types'})}}
from 
    my_events_table a

```

{% endraw %}
{% enddocs %}
