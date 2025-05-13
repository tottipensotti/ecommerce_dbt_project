{{ config(
    materialized="incremental",
    unique_key="session_id",
    alias="sessions",
    sort="created_at",
    dist="created_at")
}}

SELECT
    s.session_id,
    s.user_id,
    UPPER(s.source) AS source,
    CAST(s.session_ts AS TIMESTAMP) AS created_at
FROM
    {{ ref('sessions') }} AS s
{% if is_incremental() %}
WHERE s.session_id > (SELECT MAX(session_id) FROM {{ this }})
{% endif %}