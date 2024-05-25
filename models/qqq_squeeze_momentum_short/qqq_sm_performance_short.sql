{{ config(materialized='table') }}

with threshold_centroids as (select
	squeeze_threshold_centroid
	, momentum_threshold_centroid
	, finish_timestamp as start_timestamp
	, LAG(finish_timestamp, -1) over () as finish_timestamp
	, rn + 500 as start_row
	, LAG(rn, -1) over () + 500 as end_row
	, LN(squeeze_threshold_centroid / lag(squeeze_threshold_centroid) over (order by finish_timestamp)) as squeeze_log_change
	, LN(momentum_threshold_centroid / lag(momentum_threshold_centroid) over (order by finish_timestamp)) as momentum_log_change
from {{ ref('qqq_knn_centroids_short') }}
where knn = 1 and quantile = 0.02 and outcome = -1
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
                THEN 'long_entry'
            WHEN -t.momentum_threshold_centroid <= s.momentum AND s.momentum < 0 AND s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY s.start_row, s.timestamp) IN ('sqzOff', 'noSqz')
                THEN 'short_entry'
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
            , floor( ROW_NUMBER() OVER (ORDER BY sq.timestamp) / 500) * 500 as start_row
        FROM {{ ref('qqq_squeeze_momentum_short') }} AS sq
        order by timestamp
    ) AS s
    JOIN threshold_centroids AS t on s.start_row = t.start_row
    order by s.start_row, s.timestamp
),
signals AS (
    select
    	timestamp
    	, status_point
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
removed_null_exit as (
	select
		timestamp
		, case when status_point = 'exit' and log_return is null then null else status_point end as status_point
		, log_return
	from signals
),
label_signal as (
select
	*
	, LAG(log_return, -1) over (order by timestamp) as label
from (select * from removed_null_exit where status_point is not null)
),
feature_matrix as (
	select
		start_row
		, status.timestamp
		, label_signal.label
		, label_signal.status_point
		, date_index
		, squeeze_threshold_centroid
		, momentum_threshold_centroid
		, close_price
		, squeeze
		, cumulative_sqz_on
		, momentum
		, case when log_return is not null then log_return else 0 end as log_return
		, case when log_return is not null then round((exp(log_return) - 1) * close_price * 100 - 2, 2) else 0 end as pnl
	from status left join label_signal on status.timestamp = label_signal.timestamp
)
select
    feature_matrix.*
	, qqq.timestamp as qqq_timestamp
from {{ ref('qqq_squeeze_momentum_short') }} as qqq left join feature_matrix on qqq.timestamp = feature_matrix.timestamp