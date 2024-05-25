{{ config(
    materialized='udf',
    parameter_list='k INTEGER, quantile double precision, randomseed double precision',
    returns='table (
		finish_timestamp timestamp without time zone,
		rn INTEGER,
		squeeze_threshold_centroid numeric,
		momentum_threshold_centroid numeric,
		outcome INTEGER,
		upper_quantile double precision,
		lower_quantile double precision
		)'
) }}
    WITH scaled_cumreturn AS (
        select
            *,
            row_index as rn,
            (cumulative_return - AVG(cumulative_return) OVER (PARTITION BY row_index)) / STDDEV(cumulative_return) OVER (PARTITION BY row_index) AS scaled_return,
            row_number() over (partition by row_index) as partition_rn
        FROM {{ ref('smb_backtest') }}
        WHERE squeeze_threshold < 17
    ),
    perc as (select
        rn,
        percentile_cont(1-quantile) WITHIN GROUP (ORDER BY scaled_return) AS upper_perc,
        percentile_cont(quantile) WITHIN GROUP (ORDER BY scaled_return) AS lower_perc
    from scaled_cumreturn
    group by rn
    ),
    encoded_outcomes as (select
        scaled_cumreturn.*,
        case
            when scaled_return >= upper_perc then 1
            when scaled_return <= lower_perc then -1
            else 0
        end as encoded_outcome,
        upper_perc,
        lower_perc
    from scaled_cumreturn join perc on scaled_cumreturn.rn = perc.rn
    ),
    knn_table as (select
        origin.rn as rn,
        encoded_outcome,
        origin.squeeze_threshold as x,
        origin.momentum_threshold as y,
        target.squeeze_threshold as target_x,
        target.momentum_threshold as target_y,
        origin.partition_rn as id,
        target.partition_rn as target_id
    from scaled_cumreturn as target join encoded_outcomes as origin on target.rn = origin.rn
    ),
    distances AS (
        select
            rn,
            encoded_outcome,
            id,
            target_id,
            sqrt(pow(target_x - x, 2) + pow(target_y - y, 2)) AS distance
        FROM
            knn_table
        WHERE
            id != target_id
    ),
    ranked_distances as (select
        rn,
        encoded_outcome,
        id,
        target_id,
        distance,
        ROW_NUMBER() OVER (PARTITION BY rn, target_id ORDER BY distance) AS rank
    FROM
        distances
    ),
    final_cte as (select
        rn,
        encoded_outcome as outcome,
        target_id,
        ARRAY_AGG(id ORDER BY rank) AS nearest_neighbors
    FROM
        ranked_distances
    WHERE
        rank <= k and encoded_outcome != 0 -- K is the number of nearest neighbors you want to find; to avoid using random value, zero outcomes are removed
    GROUP BY
        rn, target_id, outcome
    )
    select
        finish_timestamp::timestamp as finish_timestamp,
        rn::int,
        round(avg(squeeze_threshold), 0) as squeeze_threshold_centroid,
        round(avg(momentum_threshold)/10, 0) * 10 as momentum_threshold_centroid,
        outcome,
        upper_perc as upper_quantile,
        lower_perc as lower_quantile
    from
        (select
            outcome,
            encoded_outcomes.*
        from final_cte join encoded_outcomes on final_cte.rn = encoded_outcomes.rn
            and final_cte.target_id = encoded_outcomes.partition_rn
        )
    group by rn, finish_timestamp, upper_quantile, lower_quantile, outcome
    order by rn, outcome