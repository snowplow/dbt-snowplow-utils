{% macro get_snowplow_delete_insert_sql(target, source, unique_key, dest_cols_csv, predicates) -%}
  
    delete from {{ target }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ source }}
    )
    {% if predicates %} and {{ predicates | join(' and ') }} {% endif %};

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    );

{%- endmacro %}
