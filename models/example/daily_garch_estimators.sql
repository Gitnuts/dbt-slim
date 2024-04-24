{{ config(materialized='table') }}

select
    (date_index + interval '1 day')::date as date_index
    , mu_estimate
    , mu_se
    , mu_t
    , mu_pvalue
    , ar1_estimate
    , ar1_se
    , ar1_t
    , ar1_pvalue
    , ma1_estimate
    , ma1_se
    , ma1_t
    , ma1_pvalue
    , omega_estimate
    , omega_se
    , omega_t
    , omega_pvalue
    , alpha1_estimate
    , alpha1_se
    , alpha1_t
    , alpha1_pvalue
    , beta1_estimate
    , beta1_se
    , beta1_t
    , beta1_pvalue
    , gamma1_estimate
    , gamma1_se
    , gamma1_t
    , gamma1_pvalue
    , skew_estimate
    , skew_se
    , skew_t
    , skew_pvalue
    , shape_estimate
    , shape_se
    , shape_t
    , shape_pvalue
from {{ source('public', 'garch_estimators_from_r') }}