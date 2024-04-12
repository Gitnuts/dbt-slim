{{ config(materialized='table') }}

with volume_stats as (
    select
        "timestamp"::timestamp as timestamp
        , "date"::date as date_index
        , case when "TRADES" is null then 1 when "TRADES" = 0 then 1 else "TRADES" end as trades
        , "TAKER_BUY_BASE" as taker_buy_base
        , "VOLUME" as volume
        , "QUOTE_VOLUME" as quote_volume
        , "TAKER_BUY_QUOTE" as taker_buy_quote
        , "OPEN" as open_price
    from {{ source('public', 'binance_features') }} bf
    order by timestamp
),
daily_volume_stats as (
    select distinct
        date_index
        , first_value(open_price) over (partition by date_index order by timestamp) as daily_open_price
        , sum(taker_buy_quote) over (partition by date_index) as daily_taker_buy_quote
        , sum(quote_volume) over (partition by date_index) as daily_quote_volume
        , avg(quote_volume / trades) over (partition by date_index) as average_amount_per_trade
        , sum(trades) over (partition by date_index) as daily_trades
    from volume_stats
    order by date_index
),
garch_and_volume as (
    select
        daily_garch_estimators.*
        , daily_taker_buy_quote / daily_quote_volume as taker_buy_ratio
        , daily_trades
        , average_amount_per_trade
        , daily_quote_volume
        , abs(daily_open_price - lag(daily_open_price, 1) over (order by daily_volume_stats.date_index)) / daily_trades as daily_kyles_lambda
    from daily_volume_stats join
        (select
            mu_estimate,
            mu_se,
            mu_t,
            mu_pvalue,
            ar1_estimate,
            ar1_se,
            ar1_t,
            ar1_pvalue,
            ma1_estimate,
            ma1_se,
            ma1_t,
            ma1_pvalue,
            omega_estimate,
            omega_se,
            omega_t,
            omega_pvalue,
            alpha1_estimate,
            alpha1_se,
            alpha1_t,
            alpha1_pvalue,
            beta1_estimate,
            beta1_se,
            beta1_t,
            beta1_pvalue,
            skew_estimate,
            skew_se,
            skew_t,
            skew_pvalue,
            shape_estimate,
            shape_se,
            shape_t,
            shape_pvalue,
            (date_index + interval '1 day')::date as date_index
         from {{ source('public', 'garch_estimators_from_r') }}) as daily_garch_estimators
        on daily_volume_stats.date_index = daily_garch_estimators.date_index
),
add_bd as (
    select
        gv.*
        , ROW_NUMBER() OVER (ORDER BY gv.date_index) AS rn
        , COUNT(*) OVER () AS total_rows
        , bitcoin_dominance
        , bitcoin_market_cap
    from garch_and_volume gv join {{ source('public', 'tradingview_features') }} bf on gv.date_index = bf.date::date
),
c as (
    select
        add_bd.*
        , "DFF" as interest_rate
        , "DTWEXBGS" as dollar_index
        , "T10YIE" as inflation_rate
    from add_bd join {{ source('public', 'fed_features') }} as fed_features
        on add_bd.date_index = fed_features."date"::date
    order by add_bd.date_index
)
select
    c.*
    , squeeze_log_change
    , momentum_log_change
    , positive_trade_perc
    , log_return
    , total_log_return
    , strategy_updated
	, case when inflation_rate >= 2 then 1 else -1 end as inflation_rate_factor
from c join {{ ref('sm_performance') }} as smp on c.date_index = smp.date_index