{{ config(materialized='table') }}

select
    (date_index + interval '1 day')::date as date_index
    , estimate_mu
    , estimate_ar1
    , estimate_ma1
    , estimate_omega
    , estimate_alpha1
    , estimate_beta1
    , estimate_gamma1
    , estimate_skew
    , estimate_shape
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