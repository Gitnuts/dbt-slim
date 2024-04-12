{% set col_name = "row_index" %}

{% set total_row_number %}
    select max("{{ col_name }}") as to_row
    from {{ ref('squeeze_momentum') }}
{% endset %}


{{
  config(
    materialized = "insert_by_partition",
    period = 5000,
    partition_field = col_name,
    start_partition = 0,
    stop_condition = "select max(row_index) from analytics.squeeze_momentum",
  )
}}

with recursive thresholds AS (
    select
    	7 AS squeeze_threshold
    	, 50 as momentum_threshold
    UNION ALL
    SELECT
    	CASE
      		WHEN momentum_threshold < 140 THEN squeeze_threshold
      	ELSE squeeze_threshold + 1
    	END,
    	CASE
      		WHEN momentum_threshold < 140 THEN momentum_threshold + 10
      	ELSE 50
    	END
  	FROM thresholds
  	WHERE squeeze_threshold < 17
),
status AS (
    SELECT
        s.timestamp,
        t.squeeze_threshold,
        t.momentum_threshold,
        s.close_price,
        s.squeeze,
        s.cumulative_sqz_on,
        s.momentum,
        s."{{ col_name }}",
        finish_timestamp,
        CASE
            WHEN t.momentum_threshold >= s.momentum AND s.momentum > 0 AND s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY t.squeeze_threshold, t.momentum_threshold, s.timestamp) IN ('sqzOff', 'noSqz')
                THEN 'short_entry'
            WHEN -t.momentum_threshold <= s.momentum AND s.momentum < 0 AND s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY t.squeeze_threshold, t.momentum_threshold, s.timestamp) IN ('sqzOff', 'noSqz')
                THEN 'long_entry'
            WHEN s.squeeze IN ('sqzOff', 'noSqz') AND
                LAG(s.squeeze) OVER (ORDER BY t.squeeze_threshold, t.momentum_threshold, s.timestamp) = 'sqzOn'
                AND s.cumulative_sqz_on <= t.squeeze_threshold
                THEN 'exit'
            WHEN s.squeeze = 'sqzOn' AND
                LAG(s.squeeze) OVER (ORDER BY t.squeeze_threshold, t.momentum_threshold, s.timestamp) = 'sqzOn'
                AND s.cumulative_sqz_on = t.squeeze_threshold
                THEN 'exit'
            ELSE NULL
        END AS status_point
    FROM (
        SELECT
            sq.timestamp,
            sq.close_price,
            sq.squeeze,
            sq.cumulative_sqz_on,
            sq.momentum,
            sq."{{ col_name }}",
            LAST_VALUE(sq.timestamp) OVER (ORDER BY "{{ col_name }}") as finish_timestamp
        FROM {{ ref('squeeze_momentum') }} AS sq -- Increase the limit to ensure enough data for the window functions
        where __PERIOD_FILTER__
    ) AS s
    CROSS JOIN thresholds AS t -- Start from a higher row number to ensure enough data for the window functions
),
updated_status AS (
	SELECT *
	FROM
		(SELECT
			*
			, ROW_NUMBER() OVER (PARTITION BY squeeze_threshold, momentum_threshold ORDER BY timestamp) AS rn
			, SUM(CASE WHEN status_point = 'exit' THEN 1 ELSE 0 END) OVER (PARTITION BY squeeze_threshold, momentum_threshold ORDER BY timestamp) AS exit_occurrence
		FROM status
		WHERE status_point IS NOT null)
	WHERE rn > exit_occurrence
),
final_cte AS (
    SELECT
        timestamp,
        finish_timestamp,
        squeeze_threshold,
        momentum_threshold,
        "{{ col_name }}",
        CASE
            WHEN status_point = 'exit' AND LAG(status_point) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp) = 'exit'
                THEN NULL
            WHEN status_point != 'exit' AND LAG(status_point) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp) = 'exit'
                THEN NULL
            WHEN status_point = 'exit' AND LAG(status_point) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp) = 'long_entry'
                THEN LN(close_price / LAG(close_price) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp))
            WHEN status_point = 'exit' AND LAG(status_point) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp) = 'short_entry'
                THEN (-1) * LN(close_price / LAG(close_price) OVER (ORDER BY squeeze_threshold, momentum_threshold, timestamp))
            ELSE NULL
        END AS log_return
    FROM updated_status
),
result_cte AS (
    SELECT
        squeeze_threshold,
        momentum_threshold,
        COUNT(log_return) AS number_of_trades,
        COUNT(CASE WHEN log_return >= 0 THEN 1 END) AS number_of_positive_trades,
        SUM(log_return) AS cumulative_return,
		"{{ col_name }}",
        finish_timestamp
    FROM final_cte
    GROUP BY finish_timestamp, "{{ col_name }}", squeeze_threshold, momentum_threshold
)
SELECT *
FROM result_cte