{% macro drop_functions() %}

DROP FUNCTION IF EXISTS {{ target.schema }}.squeeze_momentum_strategy_result(integer, double precision, double precision);
{# DROP FUNCTION IF EXISTS {{ target.schema }}.squeeze_momentum_backtest(integer, integer); #}

{% endmacro %}