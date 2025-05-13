{{ config(
    materialized="table",
    unique_key="user_id",
    dist="user_id")
}}

SELECT
    u.user_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(o.usd_amount) AS total_usd_amount,
    MIN(o.created_at) AS first_order_date,
    MAX(o.created_at) AS last_order_date
FROM
    {{ ref('stg__users')}} AS u
    LEFT JOIN {{ ref('stg__orders') }} AS o ON o.user_id = u.user_id
GROUP BY 1