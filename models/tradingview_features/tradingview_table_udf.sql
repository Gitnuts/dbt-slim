{{ config(
    materialized='udf',
    parameter_list='_tbl regclass',
    returns='table (
        date_timestamp timestamp with time zone,
		close double precision,
		open double precision
		)'
) }}
 EXECUTE
 '
 (select
    time as date_timestamp
    , lag(close, -1) over (order by time) as close
    , lag(open, -1) over (order by time) as open
from (select
        *
        , ROW_NUMBER() OVER (PARTITION BY time ORDER BY time DESC) AS rn
    from ' || _tbl || ')
where rn = 1
order by date_timestamp desc
offset 1)
union all
(select
    time as date_timestamp
    , close
    , open
from (select
        *
        , ROW_NUMBER() OVER (PARTITION BY time ORDER BY time DESC) AS rn
    from ' || _tbl || ')
where rn = 2
order by date_timestamp desc
limit 1)
order by date_timestamp desc
'