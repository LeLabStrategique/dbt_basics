{{ config(
    materialized='incremental',
    unique_key=['date_day', 'organization_id'],
    cluster_by=['date_day', 'organization_id'],
    schema='analytics',  
    tags=['marts', 'web_analytics']
) }}

WITH source_views AS (
    SELECT
        organization_id,
        day AS date_day, 
        overview_page_views,
        career_page_views,
        life_tab_page_views,
        
        (overview_page_views + career_page_views + life_tab_page_views) AS total_page_views
    FROM
        {{ ref('stg_linkedin_pages__page_view') }}
    {% if is_incremental() %}
    WHERE day >= date_sub(current_date(), interval 7 day)
    {% endif %}
)

SELECT * FROM source_views