{{ config(
    materialized="table",
    unique_key=["user_id", "session_id"],
    dist="user_id")
}}

{{ config(
    materialized="table",
    unique_key=["user_id", "session_id"],
    dist="user_id")
}}

SELECT
    u.user_id,
    s.session_id,
    s.created_at AS session_start,
    s.source AS session_source,
    CASE 
        WHEN s.created_at = (SELECT MIN(created_at) FROM {{ ref('stg__sessions') }} WHERE user_id = u.user_id)
        THEN 1 ELSE 0 
    END AS is_first_session,
    CASE 
        WHEN s.created_at = (SELECT MAX(created_at) FROM {{ ref('stg__sessions') }} WHERE user_id = u.user_id)
        THEN 1 ELSE 0 
    END AS is_last_session
FROM
    {{ ref('stg__users') }} AS u
    LEFT JOIN {{ ref('stg__sessions') }} AS s ON u.user_id = s.user_id
{{ dbt_utils.group_by(4) }}