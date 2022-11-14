{%- if target.type in ['databricks', 'spark'] %}

  with data as (

    select

      'a;b;c' as result

    union all

    select

    'a;b;c' as result
  )

  select * from data


{%- else %}

  with data as (

    select

      'a;b;c' as result

    union all

    select

    'b;a;c' as result
  )

  select * from data

    {%- endif %}
