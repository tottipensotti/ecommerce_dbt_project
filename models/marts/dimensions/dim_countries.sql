{{ config(
    materialized="incremental",
    unique_key="country_id",
    incremental_strategy="insert",
    dist="country_id")
}}

WITH new_countries AS (
    SELECT
        DISTINCT {{ dbt_utils.generate_surrogate_key(['country']) }} AS country_id,
        country
    FROM
        {{ ref('stg__users') }}
)

SELECT
    nc.country_id,
    nc.country AS name
FROM
    new_countries AS nc

{% if is_incremental() %}
    WHERE nc.country_id NOT IN (SELECT country_id FROM {{ this }})
{% endif %}
