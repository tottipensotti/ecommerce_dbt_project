{{ config(
    materialized="incremental",
    unique_key="user_id",
    alias="users",
    sort="created_at",
    dist="created_at")
}}

SELECT
    u.user_id,
    CAST(u.created_at AS TIMESTAMP) AS created_at,
    UPPER(u.country) AS country
FROM
    {{ ref('users') }} AS u
{% if is_incremental() %}
WHERE u.user_id > (SELECT MAX(user_id) FROM {{ this }})
{% endif %}