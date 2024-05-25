{{ config(materialized='table') }}

with threshold_centroids as (select
	squeeze_threshold_centroid
	, momentum_threshold_centroid
	, finish_timestamp as start_timestamp
	, LAG(finish_timestamp, -1) over () as finish_timestamp
	, rn + '{{ var("batch_size") }}' as start_row
	, LAG(rn, -1) over () + '{{ var("batch_size") }}' as end_row
	, LN(squeeze_threshold_centroid / lag(squeeze_threshold_centroid) over (order by finish_timestamp)) as squeeze_log_change
	, LN(momentum_threshold_centroid / lag(momentum_threshold_centroid) over (order by finish_timestamp)) as momentum_log_change
from {{ ref('squeeze_momentum_knn_centroids') }}
where knn = '{{ var("k_value") }}' and quantile = '{{ var("quantile") }}' and outcome = 1
),
status AS (
    select
    	s.start_row
    	, s.date_index
        , s.timestamp
        , t.squeeze_threshold_centroid
        , t.momentum_threshold_centroid
        , s.close_price
        , s.squeeze
        , s.cumulative_sqz_on
        , s.momentum
        , squeeze_log_change
        , momentum_log_change
        , CASE
            WHEN t.momentum_threshold_centroid >= s.momentum AND s.momentum > 0 AND s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY s.start_row, s.timestamp) IN ('sqzOff', 'noSqz')
                THEN 'short_entry'
            WHEN -t.momentum_threshold_centroid <= s.momentum AND s.momentum < 0 AND s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY s.start_row, s.timestamp) IN ('sqzOff', 'noSqz')
                THEN 'long_entry'
            WHEN s.squeeze IN ('sqzOff', 'noSqz') AND
                LAG(s.squeeze) OVER (ORDER BY s.start_row, s.timestamp) = 'sqzOn'
                AND s.cumulative_sqz_on <= t.squeeze_threshold_centroid
                THEN 'exit'
            WHEN s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY s.start_row, s.timestamp) = 'sqzOn'
                AND s.cumulative_sqz_on = t.squeeze_threshold_centroid
                THEN 'exit'
            ELSE NULL
        END AS status_point
    FROM (
        select
        	sq.timestamp::date as date_index
            , sq.timestamp
            , sq.close_price
            , sq.squeeze
            , sq.cumulative_sqz_on
            , sq.momentum
            , floor( ROW_NUMBER() OVER (ORDER BY sq.timestamp) / '{{ var("batch_size") }}') * '{{ var("batch_size") }}' as start_row
        FROM {{ ref('squeeze_momentum') }} AS sq
        order by timestamp
    ) AS s
    JOIN threshold_centroids AS t on s.start_row = t.start_row
    order by s.start_row, s.timestamp
),
signals AS (
    select
    	start_row
    	, date_index
        , timestamp
        , squeeze_threshold_centroid
        , momentum_threshold_centroid
        , squeeze_log_change
        , momentum_log_change
        , CASE
            WHEN status_point = 'exit' AND LAG(status_point) OVER (ORDER BY start_row, timestamp) = 'exit'
                THEN NULL
            WHEN status_point != 'exit' AND LAG(status_point) OVER (ORDER BY start_row, timestamp) = 'exit'
                THEN NULL
            WHEN status_point = 'exit' AND LAG(status_point) OVER (ORDER BY start_row, timestamp) = 'long_entry'
                THEN LN(close_price / LAG(close_price) OVER (ORDER BY start_row, timestamp))
            when status_point = 'exit' AND LAG(status_point) OVER (ORDER BY start_row, timestamp) = 'short_entry'
            	then (-1) * LN(close_price / LAG(close_price) OVER (ORDER BY start_row, timestamp))
            ELSE NULL
        END AS log_return
    FROM status
    WHERE status_point IS NOT NULL
),
results AS (
    select
    	start_row
    	, date_index
    	, squeeze_threshold_centroid
        , momentum_threshold_centroid
        , squeeze_log_change
        , momentum_log_change
        , COUNT(log_return) AS number_of_trades
        , COUNT(CASE WHEN log_return >= 0 THEN 1 END) AS number_of_positive_trades
        , SUM(log_return) AS cumulative_return
    FROM signals
    GROUP BY start_row, date_index, squeeze_threshold_centroid, momentum_threshold_centroid, squeeze_log_change, momentum_log_change
),
adjusted_results as (
select
	date_index
	, start_row
	, squeeze_threshold_centroid
	, momentum_threshold_centroid
	, squeeze_log_change
    , momentum_log_change
	, case when number_of_trades > 0 then cast(number_of_positive_trades::float / number_of_trades::float as decimal(10,2)) else 0.0 end as positive_trade_perc
	, number_of_trades
	, number_of_positive_trades
	, case when number_of_trades = 0 then 0 else cumulative_return end as log_return
	, sum(cumulative_return) over (order by date_index) as total_log_return
	, case when start_row > (LAG(start_row) over (order by date_index)) then 1 else 0 end as strategy_updated
from results
order by start_row, date_index
)
select *
from adjusted_results