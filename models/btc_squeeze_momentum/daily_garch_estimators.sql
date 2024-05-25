{{ config(materialized='table') }}

with garch_estimators as (
    select
        (date_index + interval '1 day')::date as date_index
        , lag(estimate_mu, -1) over (order by date_index) as predicted_estimate_mu
        , lag(estimate_ar1, -1) over (order by date_index) as predicted_estimate_ar1
        , lag(estimate_ma1, -1) over (order by date_index) as predicted_estimate_ma1
        , lag(estimate_omega, -1) over (order by date_index) as predicted_estimate_omega
        , lag(estimate_alpha1, -1) over (order by date_index) as predicted_estimate_alpha1
        , lag(estimate_beta1, -1) over (order by date_index) as predicted_estimate_beta1
        , lag(estimate_gamma1, -1) over (order by date_index) as predicted_estimate_gamma1
        , lag(estimate_skew, -1) over (order by date_index) as predicted_estimate_skew
        , lag(estimate_shape, -1) over (order by date_index) as predicted_estimate_shape
        , se_mu
        , se_ar1
        , se_ma1
        , se_omega
        , se_alpha1
        , se_beta1
        , se_gamma1
        , se_skew
        , se_shape
        , t_mu
        , t_ar1
        , t_ma1
        , t_omega
        , t_alpha1
        , t_beta1
        , t_gamma1
        , t_skew
        , t_shape
        , pvalue_mu
        , pvalue_ar1
        , pvalue_ma1
        , pvalue_omega
        , pvalue_alpha1
        , pvalue_beta1
        , pvalue_gamma1
        , pvalue_skew
        , pvalue_shape
    from {{ source('public', 'btc_garch_estimators') }}
)
select *
from garch_estimators