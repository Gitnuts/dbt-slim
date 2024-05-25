{{ config(materialized='table') }}

{% for knn in range(1, 3) %}
    {% for quantile in range(1, 21) %}
        SELECT
            {{ knn }} AS knn,
            {{ quantile / 100 }} AS quantile,
            *
        FROM {{ ref('squeeze_momentum_strategy_result') }}({{ knn }}, {{ quantile / 100 }}, 0.5)
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}