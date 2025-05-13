{{ config(
    materialized="table",
    unique_key="country_name",
    dist="country_name")
}}

SELECT
    c.name AS country_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(COALESCE(o.usd_amount, 0)) AS total_usd_amount,
    COUNT(DISTINCT u.user_id) AS total_users,
    COUNT(DISTINCT o.user_id) AS total_transacting_users,
    MIN(o.created_at) AS first_order_date,
    MAX(o.created_at) AS last_order_date
FROM
    {{ ref('dim_countries')}} AS c
    LEFT JOIN {{ ref('stg__users') }} AS u ON c.name = u.country
    LEFT JOIN {{ ref('stg__orders') }} AS o ON o.user_id = u.user_id
GROUP BY 1