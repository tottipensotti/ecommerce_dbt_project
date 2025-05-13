{{ config(
    materialized="table",
    unique_key="user_id",
    dist="user_id"
) }}

WITH session_metrics AS (
    SELECT
        user_id,
        session_id,
        session_source,
        is_first_session,
        is_last_session
    FROM {{ ref('fact_user_sessions') }}
),

session_counts AS (
    SELECT
        user_id,
        session_source,
        COUNT(*) AS session_count
    FROM session_metrics
    GROUP BY 1, 2
),

ranked_sources AS (
    SELECT
        user_id,
        session_source,
        RANK() OVER (PARTITION BY user_id ORDER BY session_count DESC) AS source_rank
    FROM session_counts
),

most_used_source AS (
    SELECT user_id, session_source AS most_used_source
    FROM ranked_sources
    WHERE source_rank = 1
),

sources_overview AS (
    SELECT
        user_id,
        MAX(CASE WHEN is_first_session = 1 THEN session_source END) AS first_source,
        MAX(CASE WHEN is_last_session = 1 THEN session_source END) AS last_source,
        COUNT(DISTINCT session_id) AS total_session_count,
        COUNT(DISTINCT session_source) AS distinct_sources_count
    FROM session_metrics
    GROUP BY 1
)

SELECT
    u.user_id,
    mus.most_used_source,
    so.first_source,
    so.last_source,
    so.total_session_count,
    so.distinct_sources_count
FROM
    {{ ref('stg__users') }} AS u
    LEFT JOIN sources_overview so ON so.user_id = u.user_id
    LEFT JOIN most_used_source mus ON mus.user_id = u.user_id