{{ config(
    materialized='table',
    indexes=[{'columns': ['timestamp', 'row_index'], 'unique': true}]
)}}

with moving_stats as (
	select
		CAST( AVG("CLOSE") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("bb_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS sma_20_close
		, CAST( stddev("CLOSE") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("bb_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS std_20_close
		, CAST( AVG("CLOSE") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("kc_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS kc_sma_20_close
		, CAST( MIN("LOW") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS min_20_low
		, CAST( MAX("HIGH") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS max_20_high
		, CAST( AVG("HIGH" - "LOW") over (ORDER BY timestamp ROWS BETWEEN ('{{ var("momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS sma_20_range
		, timestamp
		, "CLOSE" as close_price
		, FLOOR(sum("QUOTE_VOLUME") over (order by timestamp) / 3e10) * 3e10 as dollar_cumulative_volume
	    , FLOOR(sum("VOLUME") over (order by timestamp) / 1e6) * 1e6 as bitcoin_cumulative_volume
	from {{ source('public', 'binance_features') }}
),
moving_stats_2 as (
	select
		sma_20_close + '{{ var("bb_mult") }}' * std_20_close as upperBB
		, sma_20_close - '{{ var("bb_mult") }}' * std_20_close as lowerBB
		, kc_sma_20_close + '{{ var("kc_mult") }}' * sma_20_range as upperKC
		, kc_sma_20_close - '{{ var("kc_mult") }}' * sma_20_range as lowerKC
		, close_price - (((max_20_high + min_20_low) / 2) + sma_20_close) / 2 as linreg
		, timestamp
		, close_price
		, ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
		, dollar_cumulative_volume
		, bitcoin_cumulative_volume
	from moving_stats
),
moving_stats_3 as (
	select
		case when lowerBB > lowerKC and upperBB < upperKC then 'sqzOn'
		when lowerBB < lowerKC and upperBB > upperKC then 'sqzOff'
		else 'noSqz' end squeeze
		, timestamp
		, close_price
		, rn
		, dollar_cumulative_volume
		, bitcoin_cumulative_volume
		, regr_slope(linreg, rn) OVER (ORDER BY rn ROWS BETWEEN ('{{ var("momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS slope
		, regr_intercept(linreg, rn) OVER (ORDER BY rn ROWS BETWEEN ('{{ var("momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS intercept
	from moving_stats_2
),
final as (
	select
	    timestamp
	    , squeeze
		, close_price
		, cast((intercept + slope * rn) as numeric(10,2) ) as momentum
		, dollar_cumulative_volume
		, bitcoin_cumulative_volume
	from moving_stats_3
),
squeeze_on AS (
    SELECT
        CASE WHEN squeeze = 'sqzOn' THEN 1 ELSE 0 END AS sqz_on
        , timestamp
        , ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
    FROM final
),
cumulative_squeeze_on AS (
    SELECT
        timestamp
        , sqz_on
        , SUM(sqz_on) OVER (ORDER BY timestamp) AS cumulative_sum
        , LAG(sqz_on) OVER (ORDER BY timestamp) AS lagged_sqz_on
    FROM squeeze_on
),
squeeze_off AS (
    SELECT
        timestamp
        , sqz_on
        , cumulative_sum
        , CASE WHEN sqz_on = 0 AND lagged_sqz_on = 1 THEN cumulative_sum END AS sqz_off
    FROM cumulative_squeeze_on
),
count_squeeze_off AS (
    SELECT
        *
        , COUNT(sqz_off) OVER (ORDER BY timestamp) AS rn
    FROM squeeze_off
),
cumulative_sqz_off AS (
    SELECT
        timestamp
        , cumulative_sum
        , sqz_on
        , COALESCE(MAX(sqz_off) OVER (PARTITION BY rn), 0) AS cumulative_sqz_off
    FROM count_squeeze_off
)
SELECT
    final.*
    , cumulative_sum - cumulative_sqz_off AS cumulative_sqz_on
    , last_value(final."timestamp") over (partition by
        TO_CHAR(DATE_TRUNC('day', final."timestamp"::timestamp) + INTERVAL '1 day', 'IYYY-IW')
        )::timestamp as date_index
    , FLOOR((ROW_NUMBER() OVER
                (ORDER BY final."timestamp") - 1) / 5000) * 5000 AS row_index
FROM final join cumulative_sqz_off on final.timestamp = cumulative_sqz_off.timestamp
