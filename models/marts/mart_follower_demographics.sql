{{ config(
    materialized='incremental',
    unique_key=['date_day', 'organization_id', 'dimension_type', 'dimension_name'],
    cluster_by=['date_day', 'organization_id'],
    schema='analytics',  
    tags=['marts', 'demographics']
) }}

WITH source_followers AS (
    SELECT
        organization_id,
        day AS date_day, 
        follower_count,
        dimension_type,
        dimension_name
    FROM
        {{ ref('stg_linkedin_pages__follower_statistic') }}
    {% if is_incremental() %}
    WHERE day >= date_sub(current_date(), interval 7 day) 
    {% endif %}
),

final AS (
    SELECT
        -- CORRECTION: Utilisation de la nouvelle macro dbt_utils
        {{ dbt_utils.generate_surrogate_key(['date_day', 'organization_id', 'dimension_type', 'dimension_name']) }} AS follower_sk,
        organization_id,
        date_day,
        dimension_type,
        dimension_name,
        follower_count
    FROM source_followers
)

SELECT * FROM final