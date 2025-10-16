{{
    config(
        materialized='incremental',
        unique_key='day', 
        incremental_strategy='merge', 
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        -- Colonnes de la source f (explicites)
        f.organization_entity, -- URN de l'organisation (CONFIRMÉ)
        f.day AS day_raw_timestamp, -- Timestamp de la date d'agrégation
        f._fivetran_synced,
        
        -- ************************************************************
        -- * 1. CLICS *
        -- ************************************************************
        f.careers_page_promo_links_clicks,
        f.careers_page_banner_promo_clicks,
        f.careers_page_jobs_clicks,
        f.careers_page_employees_clicks,
        f.mobile_careers_page_promo_links_clicks,
        f.mobile_careers_page_jobs_clicks,
        f.mobile_careers_page_employees_clicks,

        -- ************************************************************
        -- * 2. VUES AGRÉGÉES (Total et Unique) *
        -- ************************************************************
        f.all_desktop_page_views,
        f.all_desktop_unique_page_views,
        f.all_mobile_page_views,
        f.all_mobile_unique_page_views,
        f.all_page_views,
        f.all_unique_page_views,

        -- ************************************************************
        -- * 3. VUES PAR PAGE (Détaillées) *
        -- ************************************************************
        f.about_page_views,
        f.about_unique_page_views,
        f.careers_page_views,
        f.careers_unique_page_views,
        f.products_page_views,
        f.products_unique_page_views,
        f.jobs_page_views,
        f.jobs_unique_page_views,
        f.people_page_views,
        f.people_unique_page_views,
        f.overview_page_views,
        f.overview_unique_page_views,
        f.life_at_page_views,
        f.life_at_unique_page_views,
        f.insights_page_views,
        f.insights_unique_page_views,

        -- ************************************************************
        -- * 4. VUES PAR PAGE (Mobile) *
        -- ************************************************************
        f.mobile_careers_page_views,
        f.mobile_careers_unique_page_views,
        f.mobile_overview_page_views,
        f.mobile_overview_unique_page_views,
        f.mobile_jobs_page_views,
        f.mobile_jobs_unique_page_views,
        f.mobile_life_at_page_views,
        f.mobile_life_at_unique_page_views,
        f.mobile_insights_page_views,
        f.mobile_insights_unique_page_views,
        f.mobile_products_page_views,
        f.mobile_products_unique_page_views,
        f.mobile_about_page_views,
        f.mobile_about_unique_page_views,
        f.mobile_people_page_views,
        f.mobile_people_unique_page_views,
        
        -- ************************************************************
        -- * 5. VUES PAR PAGE (Desktop) *
        -- ************************************************************
        f.desktop_insights_page_views,
        f.desktop_insights_unique_page_views,
        f.desktop_careers_page_views,
        f.desktop_careers_unique_page_views,
        f.desktop_life_at_page_views,
        f.desktop_life_at_unique_page_views,
        f.desktop_jobs_page_views,
        f.desktop_jobs_unique_page_views,
        f.desktop_people_page_views,
        f.desktop_people_unique_page_views,
        f.desktop_about_page_views,
        f.desktop_about_unique_page_views,
        f.desktop_overview_page_views,
        f.desktop_overview_unique_page_views,
        f.desktop_products_page_views,
        f.desktop_products_unique_page_views,

        -- Colonnes temporelles calculées
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp, 
        CAST(f.day AS DATE) AS day_format,
        CAST(DATE(f.day) AS STRING FORMAT 'YYYY-MM-DD') AS day_format_string
        
    FROM 
        {{ source('fivetran_linkedin', 'time_bound_page_statistic') }} AS f

    {% if is_incremental() %}
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        s.*, -- Sélectionne toutes les colonnes de source_data (y compris organization_entity)
        
        -- Construction de la clé analytique (row_id)
        CONCAT(
            CAST(DATE(s.extract_timestamp_temp) AS STRING FORMAT 'YYYY-MM-DD'), 
            '_', 
            s.day_format_string
        ) AS row_id,

        ROW_NUMBER() OVER (
            PARTITION BY 
                s.day_format
            ORDER BY s.extract_timestamp_temp DESC
        ) AS rank_by_recency

    FROM 
        source_data AS s
)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE (Standardisée) **
    -- ************************************************************
    
    -- URN Organisation (Maintenant disponible)
    d.organization_entity AS _organization_entity_urn, 
    
    -- ROW_ID
    d.row_id,
    
    -- TIMESTAMP de la synchro
    d.extract_timestamp_temp AS extract_timestamp,
    
    -- DIM_DAY (TIMESTAMP brut du jour d'agrégation)
    d.day_raw_timestamp AS dim_day, 
    
    -- DIM_DAY_ID
    CAST(NULL AS STRING) AS dim_day_id, 
    
    -- DAY (Date d'agrégation au format YYYY-MM-DD)
    d.day_format AS day, 
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS (Métriques de Clics et Vues) **
    -- ************************************************************

    -- Clics
    d.careers_page_promo_links_clicks,
    d.careers_page_banner_promo_clicks,
    d.careers_page_jobs_clicks,
    d.careers_page_employees_clicks,
    d.mobile_careers_page_promo_links_clicks,
    d.mobile_careers_page_jobs_clicks,
    d.mobile_careers_page_employees_clicks,

    -- Vues agrégées
    d.all_desktop_page_views,
    d.all_desktop_unique_page_views,
    d.all_mobile_page_views,
    d.all_mobile_unique_page_views,
    d.all_page_views,
    d.all_unique_page_views,

    -- Vues détaillées
    d.about_page_views,
    d.about_unique_page_views,
    d.careers_page_views,
    d.careers_unique_page_views,
    d.products_page_views,
    d.products_unique_page_views,
    d.jobs_page_views,
    d.jobs_unique_page_views,
    d.people_page_views,
    d.people_unique_page_views,
    d.overview_page_views,
    d.overview_unique_page_views,
    d.life_at_page_views,
    d.life_at_unique_page_views,
    d.insights_page_views,
    d.insights_unique_page_views,

    -- Vues Mobile
    d.mobile_careers_page_views,
    d.mobile_careers_unique_page_views,
    d.mobile_overview_page_views,
    d.mobile_overview_unique_page_views,
    d.mobile_jobs_page_views,
    d.mobile_jobs_unique_page_views,
    d.mobile_life_at_page_views,
    d.mobile_life_at_unique_page_views,
    d.mobile_insights_page_views,
    d.mobile_insights_unique_page_views,
    d.mobile_products_page_views,
    d.mobile_products_unique_page_views,
    d.mobile_about_page_views,
    d.mobile_about_unique_page_views,
    d.mobile_people_page_views,
    d.mobile_people_unique_page_views,
    
    -- Vues Desktop
    d.desktop_insights_page_views,
    d.desktop_insights_unique_page_views,
    d.desktop_careers_page_views,
    d.desktop_careers_unique_page_views,
    d.desktop_life_at_page_views,
    d.desktop_life_at_unique_page_views,
    d.desktop_jobs_page_views,
    d.desktop_jobs_unique_page_views,
    d.desktop_people_page_views,
    d.desktop_people_unique_page_views,
    d.desktop_about_page_views,
    d.desktop_about_unique_page_views,
    d.desktop_overview_page_views,
    d.desktop_overview_unique_page_views,
    d.desktop_products_page_views,
    d.desktop_products_unique_page_views
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1