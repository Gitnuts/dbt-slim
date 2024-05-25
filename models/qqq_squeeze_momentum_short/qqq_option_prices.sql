{{ config(
    materialized='table'
)}}

with adjusted_iv_table as ( -- this is to adjust the timestamp to the true trading hours
	select
		case when start_of_trading_hours = '10:30:00'
		then iv.timestamp - interval '1 hour'
		else iv.timestamp end as timestamp
		, iv.open
		, iv.high
		, iv.low
		, iv.close
	from {{ source('public','qqq_implied_volatility') }} iv join (
		select
			min(timestamp::time) over (partition by timestamp::date) as start_of_trading_hours
			, timestamp
		from {{ source('public','qqq_implied_volatility') }}
	) timeshifted on iv.timestamp = timeshifted.timestamp
	order by iv.timestamp
),
time_diff as ( -- this is to calculate the time to maturity in years. Expiration is 4 weeks on Friday at 3:55 PM, which is approximately 30 days (28-32 days)
	SELECT
		qqq.*
	    , EXTRACT(EPOCH FROM (MAKE_TIMESTAMP(
        EXTRACT(YEAR FROM timestamp)::int,
        EXTRACT(MONTH FROM timestamp)::int,
        EXTRACT(DAY FROM timestamp)::int,
        15, 55, 0
    	) + INTERVAL '4 weeks' +
	    CASE
	        WHEN EXTRACT(DOW FROM timestamp) <= 5 THEN
	            INTERVAL '5 days' - INTERVAL '1 day' * EXTRACT(DOW FROM timestamp)
	        ELSE
	            INTERVAL '5 days' + INTERVAL '1 day' * (7 - EXTRACT(DOW FROM timestamp))
	    END - timestamp)) / (365 * 24 * 60 * 60) AS time_to_maturity_30
	FROM {{ source('public','qqq_features') }} as qqq
),
strike_prices as ( -- this is to get the strike prices and the risk free rate
	select
		timestamp::date as date
		, round("open"::numeric, 0) as strike_price
		, fed_funds_rate/100 as risk_free_rate
	from {{ source('public','qqq_features') }} qqq join {{ ref('adj_fed_features') }} fed on qqq.timestamp::date = fed.date::date
	where EXTRACT(HOUR FROM timestamp) = 10 and EXTRACT(minute FROM timestamp) = 0
),
implied_volatility_stats as ( -- this is to get the implied volatility stats
	select
		qqq.*
		, iv.close as iv_close
		, iv.open as iv_open
		, iv.high as iv_high
		, iv.low as iv_low
	from time_diff qqq join (
		select *
		from adjusted_iv_table
		order by timestamp
	) as iv on qqq.timestamp = iv.timestamp  -- beware of true trading hours. true trading hours come from qqq_implied_volatility table
),
ds as ( -- this is to calculate d1 and d2 for option prices
	select
		iv.*
		, strike_price
		, risk_free_rate
		, LN(close::numeric/strike_price) + (risk_free_rate + 0.5 * POWER(iv_close, 2) * time_to_maturity_30)/(iv_close * SQRT(time_to_maturity_30)) as d1_close
		, LN(close::numeric/strike_price) + (risk_free_rate - 0.5 * POWER(iv_close, 2) * time_to_maturity_30)/(iv_close * SQRT(time_to_maturity_30)) as d2_close
	from implied_volatility_stats as iv left join strike_prices on iv.timestamp::date = strike_prices.date
),
option_prices as ( -- this is to calculate the option prices
	select
		*
		, close::numeric * (ERF(d1_close / SQRT(2)) + 1) / 2 - strike_price * EXP(-risk_free_rate * time_to_maturity_30) * (ERF(d2_close / SQRT(2)) + 1) / 2 as call_close_price
		, strike_price * EXP(-risk_free_rate * time_to_maturity_30) * (ERF(-d2_close / SQRT(2)) + 1) / 2 - close::numeric * (ERF(-d1_close / SQRT(2)) + 1) / 2 as put_close_price
	from ds
)
select -- this is to calculate the greeks
	*
	, case when close::numeric != lag(close::numeric, 1) over (order by timestamp)
	then (call_close_price - lag(call_close_price, 1) over (order by timestamp)) /
			(close::numeric - lag(close::numeric, 1) over (order by timestamp))
	else null end as delta
	, (call_close_price - lag(call_close_price, 1) over (order by timestamp)) /
		(time_to_maturity_30 - lag(time_to_maturity_30, 1) over (order by timestamp)) as theta
	, case when iv_close != lag(iv_close, 1) over (order by timestamp)
	then (call_close_price - lag(call_close_price, 1) over (order by timestamp)) /
			(100*(iv_close - lag(iv_close, 1) over (order by timestamp)))
	else null end as vega
from option_prices