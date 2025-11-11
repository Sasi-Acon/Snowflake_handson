{{ config(materialized='dynamic_table',target_lag='1 minute',snowflake_warehouse='COMPUTE_WH')}}
 

SELECT
    country,
    COUNT(*) as signup_count
FROM
    {{ ref('stg_user_signups') }}
GROUP BY
    country