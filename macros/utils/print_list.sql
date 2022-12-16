{% macro print_list(list, separator = ',') %}

  {%- for item in list %} '{{item}}' {%- if not loop.last %}{{separator}}{% endif %} {% endfor -%}

{% endmacro %}
