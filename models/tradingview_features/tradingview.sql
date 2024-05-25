{{ config(materialized='table') }}

{% set tradingview_features = {
    "sp500_above_50_day_average_tradingview_features":"sp500_above_50",
    "sp500_above_200_day_average_tradingview_features":"sp500_above_200",
    "sp500_above_5_day_average_tradingview_features":"sp500_above_5",
    "sp500_above_20_day_average_tradingview_features":"sp500_above_20",
    "us_stocks_block_trades_tradingview_features":"us_stocks_block_trades",
    "pmi_tradingview_features":"pmi",
    "put_call_ratio_spx_tradingview_features":"put_call_ratio_spx",
    "put_call_ratio_cboe_tradingview_features":"put_call_ratio_cboe",
    "us_stocks_above_vwap_tradingview_features":"us_stocks_above_vwap",
    "total_advance_decline_ratio_tradingview_features":"total_advance_decline_ratio",
    "us_stocks_advance_decline_ratio_tradingview_features":"us_stocks_advance_decline_ratio",
    "call_volume_spx_tradingview_features":"call_volume_spx",
    "put_volume_spx_tradingview_features":"put_volume_spx",
    "call_volume_cboe_tradingview_features":"call_volume_cboe",
    "put_volume_cboe_tradingview_features":"put_volume_cboe",
    "us_stocks_tick_of_trades_tradingview_features":"us_stocks_tick_of_trades",
    "us_stocks_number_of_trades_tradingview_features":"us_stocks_number_of_trades",
} %}



WITH RECURSIVE date_range AS (
    SELECT
        DATE '2018-01-01' AS date
    UNION ALL
    SELECT
        (date + INTERVAL '1 day')::DATE
    FROM
        date_range
    WHERE
        date + INTERVAL '1 day' <= CURRENT_DATE
)
SELECT
    date,
    {% for feature, symbol in tradingview_features.items() %}
    {{ symbol }}.close as {{ symbol }}_close ,
    {{ symbol }}.open as {{ symbol }}_open {% if not loop.last %},{% endif %}
    {% endfor %}
FROM
    date_range LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "sp500_above_50_day_average_tradingview_features") }}')
    sp500_above_50 on date = sp500_above_50.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "sp500_above_200_day_average_tradingview_features") }}')
    sp500_above_200 on date = sp500_above_200.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "sp500_above_5_day_average_tradingview_features") }}')
    sp500_above_5 on date = sp500_above_5.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "sp500_above_20_day_average_tradingview_features") }}')
    sp500_above_20 on date = sp500_above_20.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "us_stocks_block_trades_tradingview_features") }}')
    us_stocks_block_trades on date = us_stocks_block_trades.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "pmi_tradingview_features") }}')
    pmi on date = pmi.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "put_call_ratio_spx_tradingview_features") }}')
    put_call_ratio_spx on date = put_call_ratio_spx.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "put_call_ratio_cboe_tradingview_features") }}')
    put_call_ratio_cboe on date = put_call_ratio_cboe.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "us_stocks_above_vwap_tradingview_features") }}')
    us_stocks_above_vwap on date = us_stocks_above_vwap.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "total_advance_decline_ratio_tradingview_features") }}')
    total_advance_decline_ratio on date = total_advance_decline_ratio.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "us_stocks_advance_decline_ratio_tradingview_features") }}')
    us_stocks_advance_decline_ratio on date = us_stocks_advance_decline_ratio.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "call_volume_spx_tradingview_features") }}')
    call_volume_spx on date = call_volume_spx.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "put_volume_spx_tradingview_features") }}')
    put_volume_spx on date = put_volume_spx.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "call_volume_cboe_tradingview_features") }}')
    call_volume_cboe on date = call_volume_cboe.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "put_volume_cboe_tradingview_features") }}')
    put_volume_cboe on date = put_volume_cboe.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "us_stocks_tick_of_trades_tradingview_features") }}')
    us_stocks_tick_of_trades on date = us_stocks_tick_of_trades.date_timestamp::date LEFT JOIN
    {{ ref('tradingview_table_udf') }}('{{ source("public", "us_stocks_number_of_trades_tradingview_features") }}')
    us_stocks_number_of_trades on date = us_stocks_number_of_trades.date_timestamp::date
ORDER BY date DESC