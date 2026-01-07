{% macro config_meta_get(key, default=none) %}
    {%- set meta_dict = config.get("meta", {}) -%}
    
    {%- if key in meta_dict -%}
        {{ return(meta_dict.get(key)) }}
    {%- elif config.get(key) != none -%}
        {{ return(config.get(key)) }}
    {%- else -%}
        {{ return(default) }}
    {%- endif -%}
{% endmacro %}


{% macro config_meta_require(key) %}
    {# the first case is required to avoid errors #}
    {%- if config == {} -%}
        {{ return(none) }}
    {# Check meta first to satisfy Fusion and stop Core 1.10+ lookup warnings #}
    {%- elif config.get("meta") != none and key in config.get("meta", {}) -%}
        {{ return(config.get("meta").get(key)) }}
    {# Fallback to top-level if not in meta - for core #}
    {%- elif config.get(key) != none -%}
        {{ return(config.get(key)) }}
    {%- else -%}
        {% do exceptions.raise_compiler_error("Configuration '" ~ key ~ "' is required but was not found under config or meta (Fusion requires custom configuration under meta)") %}
    {%- endif -%}
{% endmacro %}
