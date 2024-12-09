{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}

SELECT 'dummy_model_empty' AS test_case, cast('9999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}) as lower_limit, cast('9999-01-02 00:00:00.000' as {{ dbt.type_timestamp() }}) AS upper_limit
UNION ALL
SELECT 'dummy_model_only_nulls', cast('9999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}) , cast('9999-01-02 00:00:00.000' as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_standard', cast('2024-12-01 00:00:00.000' as {{ dbt.type_timestamp() }}) , cast('2024-12-02 00:00:00.000' as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_empty_with_lower_output_true', cast('1999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('1999-01-02 00:00:00.000'  as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_empty_with_lower_output_false', cast('9999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('9999-01-02 00:00:00.000' as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_only_nulls_with_lower_output_true', cast('1999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('1999-01-02 00:00:00.000'  as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_only_nulls_with_lower_output_false', cast('9999-01-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('9999-01-02 00:00:00.000' as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_standard_with_lower_output_true', cast('2024-12-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('2024-12-02 00:00:00.000' as {{ dbt.type_timestamp() }})
UNION ALL
SELECT 'dummy_model_standard_with_lower_output_false', cast('2024-12-01 00:00:00.000' as {{ dbt.type_timestamp() }}), cast('2024-12-02 00:00:00.000' as {{ dbt.type_timestamp() }})
