{{ config(
    materialized='table'
)}}

with moving_stats as (
	select
		CAST( AVG("close"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_bb_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS sma_20_close
		, CAST( stddev("close"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_bb_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS std_20_close
		, CAST( AVG("close"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_kc_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS kc_sma_20_close
		, CAST( MIN("low"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS min_20_low
		, CAST( MAX("high"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS max_20_high
		, CAST( AVG("high"::numeric - "low"::numeric) over (ORDER BY timestamp ROWS BETWEEN ('{{ var("dis_momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS numeric(10,2) ) AS sma_20_range
		, timestamp
		, "close"::numeric as close_price
	from {{ source('public', 'dis_features') }}
	order by timestamp
),
moving_stats_2 as (
	select
		sma_20_close + '{{ var("dis_bb_mult") }}' * std_20_close as upperBB
		, sma_20_close - '{{ var("dis_bb_mult") }}' * std_20_close as lowerBB
		, kc_sma_20_close + '{{ var("dis_kc_mult") }}' * sma_20_range as upperKC
		, kc_sma_20_close - '{{ var("dis_kc_mult") }}' * sma_20_range as lowerKC
		, close_price - (((max_20_high + min_20_low) / 2) + sma_20_close) / 2 as linreg
		, timestamp
		, close_price
		, ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
	from moving_stats
	order by timestamp
),
moving_stats_3 as (
	select
		case when lowerBB > lowerKC and upperBB < upperKC then 'sqzOn'
		when lowerBB < lowerKC and upperBB > upperKC then 'sqzOff'
		else 'noSqz' end squeeze
		, timestamp
		, close_price
		, rn
		, regr_slope(linreg, rn) OVER (ORDER BY rn ROWS BETWEEN ('{{ var("dis_momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS slope
		, regr_intercept(linreg, rn) OVER (ORDER BY rn ROWS BETWEEN ('{{ var("dis_momentum_length") }}' - 1) PRECEDING AND CURRENT ROW) AS intercept
	from moving_stats_2
	order by timestamp
),
moving_stats_4 as (
	select
		timestamp
		, close_price
		, rn
		, slope
		, intercept
		, case WHEN EXTRACT(HOUR FROM timestamp) = 9
             	AND EXTRACT(MINUTE FROM timestamp) >= 30
             	AND EXTRACT(MINUTE FROM timestamp) < 60 THEN 'sqzOff'
               WHEN EXTRACT(HOUR FROM timestamp) = 15
             	AND EXTRACT(MINUTE FROM timestamp) >= 30
             	AND EXTRACT(MINUTE FROM timestamp) < 60 THEN 'sqzOff'
               else squeeze end squeeze
     from moving_stats_3
     order by timestamp
),
final as (
	select
	    timestamp
	    , squeeze
		, close_price
		, cast((intercept + slope * rn) as numeric(10,2) ) as momentum
	from moving_stats_4
	order by timestamp
),
squeeze_on AS (
    SELECT
        CASE WHEN squeeze = 'sqzOn' THEN 1 ELSE 0 END AS sqz_on
        , timestamp
        , ROW_NUMBER() OVER (ORDER BY timestamp) AS rn
    FROM final
    order by timestamp
),
cumulative_squeeze_on AS (
    SELECT
        timestamp
        , sqz_on
        , SUM(sqz_on) OVER (ORDER BY timestamp) AS cumulative_sum
        , LAG(sqz_on) OVER (ORDER BY timestamp) AS lagged_sqz_on
    FROM squeeze_on
    order by timestamp
),
squeeze_off AS (
    SELECT
        timestamp
        , sqz_on
        , cumulative_sum
        , CASE WHEN sqz_on = 0 AND lagged_sqz_on = 1 THEN cumulative_sum END AS sqz_off
    FROM cumulative_squeeze_on
    order by timestamp
),
count_squeeze_off AS (
    SELECT
        *
        , COUNT(sqz_off) OVER (ORDER BY timestamp) AS rn
    FROM squeeze_off
    order by timestamp
),
cumulative_sqz_off AS (
    SELECT
        timestamp
        , cumulative_sum
        , sqz_on
        , COALESCE(MAX(sqz_off) OVER (PARTITION BY rn), 0) AS cumulative_sqz_off
    FROM count_squeeze_off
    order by timestamp
),
final_2 as (
    SELECT
        final.*
        , cumulative_sum - cumulative_sqz_off AS cumulative_sqz_on
        , FLOOR((ROW_NUMBER() OVER
                    (ORDER BY final."timestamp") - 1) / 500) * 500 AS row_index
    FROM final join cumulative_sqz_off on final.timestamp = cumulative_sqz_off.timestamp
    order by timestamp
)
select *
from final_2
--where row_index < 124000
where row_index < (SELECT MAX(row_index) FROM final_2) --exclude the last row_index
order by timestamp