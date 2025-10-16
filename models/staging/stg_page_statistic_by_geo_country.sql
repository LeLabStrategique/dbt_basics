{{
    config(
        materialized='incremental',
        unique_key='row_id',
        incremental_strategy='merge', 
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        f.* EXCEPT(_fivetran_id),
        d.value AS geo_country_name_enriched
        
    FROM 
        {{ source('fivetran_linkedin', 'page_statistic_by_geo_country') }} AS f
        
    LEFT JOIN
        {{ source('fivetran_linkedin', 'geo') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.geo_id AS STRING)

    {% if is_incremental() %}
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(stat_day_time) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        *,
        
        CONCAT(
            CAST(DATE(TIMESTAMP(_fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            COALESCE(geo_country_name_enriched, CAST(geo_id AS STRING)) 
        ) AS row_id,

        ROW_NUMBER() OVER (
            PARTITION BY 
                CONCAT(
                    CAST(DATE(TIMESTAMP(_fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
                    ' - ', 
                    COALESCE(geo_country_name_enriched, CAST(geo_id AS STRING)) 
                )
            ORDER BY TIMESTAMP(_fivetran_synced) DESC
        ) AS rank_by_recency

    FROM 
        source_data
)

SELECT
    row_id,
    TIMESTAMP(_fivetran_synced) AS stat_day_time,
    geo_id,
    COALESCE(geo_country_name_enriched, CAST(geo_id AS STRING)) AS dim_split_geo_country,
    all_desktop_page_views,
    all_mobile_page_views,
    all_page_views,
    about_page_views,
    careers_page_views,
    products_page_views,
    jobs_page_views,
    people_page_views,
    overview_page_views,
    life_at_page_views,
    insights_page_views,
    mobile_careers_page_views,
    mobile_overview_page_views,
    mobile_jobs_page_views,
    mobile_life_at_page_views,
    mobile_insights_page_views,
    mobile_products_page_views,
    mobile_about_page_views,
    mobile_people_page_views,
    desktop_insights_page_views,
    desktop_careers_page_views,
    desktop_life_at_page_views,
    desktop_jobs_page_views,
    desktop_people_page_views,
    desktop_about_page_views,
    desktop_overview_page_views,
    desktop_products_page_views,
    _organization_entity_urn
    
FROM 
    deduplication
WHERE
    rank_by_recency = 1