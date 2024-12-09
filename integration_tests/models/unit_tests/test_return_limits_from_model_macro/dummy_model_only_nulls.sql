SELECT cast(NULL as {{ dbt.type_timestamp() }}) AS tstamp_col
UNION ALL
SELECT cast(NULL as {{ dbt.type_timestamp() }}) AS tstamp_col
