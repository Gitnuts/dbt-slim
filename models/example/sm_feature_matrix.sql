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
add_garch_estimators as (
    select
        daily_garch_estimators.*
        , daily_taker_buy_quote / daily_quote_volume as taker_buy_ratio
        , daily_trades
        , average_amount_per_trade
        , daily_quote_volume
        , abs(daily_open_price - lag(daily_open_price, 1) over (order by daily_volume_stats.date_index)) / daily_trades as daily_kyles_lambda
    from daily_volume_stats join {{ ref('daily_garch_estimators') }} as daily_garch_estimators
        on daily_volume_stats.date_index = daily_garch_estimators.date_index
),
add_tradingview_features as (
    select
        addge.*
        , ROW_NUMBER() OVER (ORDER BY addge.date_index) AS rn
        , COUNT(*) OVER () AS total_rows
        , bitcoin_dominance
        , bitcoin_market_cap
    from add_garch_estimators addge join {{ source('public', 'tradingview_features') }} tf on addge.date_index = tf.date::date
),
add_fed_features as (
    select
        add_tradingview_features.*
        , "DFF" as interest_rate
        , "DTWEXBGS" as dollar_index
        , "T10YIE" as inflation_rate
    from add_tradingview_features join {{ source('public', 'fed_features') }} as fed_features
        on add_tradingview_features.date_index = fed_features."date"::date
    order by add_tradingview_features.date_index
),
add_sm_performance as (
    select
        add_fed_features.*
        , log_return
        , number_of_trades
        , number_of_positive_trades
        , case when inflation_rate >= 2 then 1 else -1 end as inflation_rate_factor
    from add_fed_features join (
        select
            date_index
            , sum(log_return) as log_return
            , sum(number_of_trades) as number_of_trades
            , sum(number_of_positive_trades) as number_of_positive_trades
        from {{ ref('sm_performance') }}
        group by date_index
        order by date_index) as smp
        on add_fed_features.date_index = smp.date_index
),
add_garch_sigma as (
    select
		addsmp.*
		, mu
		, sigma
		, skew
		, shape
		, realized
		, sign( lag(log_return, -1) over (order by addsmp.date_index) ) as sign_log_return
	from add_sm_performance addsmp join {{ source('public', 'btc_garch_sigma') }} gsfr on addsmp.date_index = gsfr.date_index
)
select
    *
    , EXTRACT(DOW FROM date_index) AS day_of_week_number
from add_garch_sigma