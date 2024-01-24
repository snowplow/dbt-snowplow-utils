{#
Copyright (c) 2021-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Personal and Academic License Version 1.0,
and you may not use this file except in compliance with the Snowplow Personal and Academic License Version 1.0.
You may obtain a copy of the Snowplow Personal and Academic License Version 1.0 at https://docs.snowplow.io/personal-and-academic-license-1.0/
#}
{% macro get_optional_fields(enabled, fields, col_prefix, relation, relation_alias, include_field_alias=true) -%}

  {%- if enabled -%}

    {%- set combined_fields = snowplow_utils.combine_column_versions(
                                    relation=relation,
                                    column_prefix=col_prefix,
                                    required_fields=fields|map(attribute='field')|list,
                                    relation_alias=relation_alias,
                                    include_field_alias=include_field_alias
                                    ) -%}

    {{ combined_fields|join(',\n') }}

  {%- else -%}

    {% for field in fields %}

      {%- set field_alias = snowplow_utils.get_field_alias(field.field)[1] -%}

      cast(null as {{ field.dtype }}){%- if include_field_alias %} as {{ field_alias }}{%- endif %} {%- if not loop.last %}, {% endif %}
    {% endfor %}

  {%- endif -%}

{% endmacro %}
