{{ config(
    materialized="incremental",
    unique_key="order_id",
    alias="orders",
    sort="created_at",
    dist="created_at")
}}

SELECT
    o.order_id,
    o.user_id,
    CAST(o.order_ts AS TIMESTAMP) AS created_at,
    CAST(o.amount_usd AS DECIMAL(16,2)) AS usd_amount
FROM
    {{ ref('orders') }} AS o
{% if is_incremental() %}
WHERE o.order_id > (SELECT MAX(order_id) FROM {{ this }})
{% endif %}