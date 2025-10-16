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
        -- Colonnes de la source f (explicites, remplacement de f.* EXCEPT)
        f._organization_entity_urn,
        f.geo_id,
        f._fivetran_synced,
        f.all_desktop_page_views,
        f.all_mobile_page_views,
        f.all_page_views,
        f.about_page_views,
        f.careers_page_views,
        f.products_page_views,
        f.jobs_page_views,
        f.people_page_views,
        f.overview_page_views,
        f.life_at_page_views,
        f.insights_page_views,
        f.mobile_careers_page_views,
        f.mobile_overview_page_views,
        f.mobile_jobs_page_views,
        f.mobile_life_at_page_views,
        f.mobile_insights_page_views,
        f.mobile_products_page_views,
        f.mobile_about_page_views,
        f.mobile_people_page_views,
        f.desktop_insights_page_views,
        f.desktop_careers_page_views,
        f.desktop_life_at_page_views,
        f.desktop_jobs_page_views,
        f.desktop_people_page_views,
        f.desktop_about_page_views,
        f.desktop_overview_page_views,
        f.desktop_products_page_views,
        
        -- Colonne de dimension enrichie
        d.value AS dim_geo_name_enriched,
        
        -- Colonnes temporelles calculées
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp,
        CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format
        
    FROM 
        {{ source('fivetran_linkedin', 'page_statistic_by_geo') }} AS f
        
    LEFT JOIN
        {{ source('fivetran_linkedin', 'geo') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.geo_id AS STRING)

    -- FILTRE D'OPTIMISATION (Incrémentalité)
    {% if is_incremental() %}
        -- Nous utilisons le nom de la colonne dans la table cible (extract_timestamp)
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        s.*, -- Sélectionne toutes les colonnes de source_data
        
        -- Crée la clé analytique (row_id)
        CONCAT(
            s.day_format, 
            '_', 
            COALESCE(s.dim_geo_name_enriched, CAST(s.geo_id AS STRING)) 
        ) AS row_id,

        ROW_NUMBER() OVER (
            PARTITION BY 
                s.day_format, 
                COALESCE(s.dim_geo_name_enriched, CAST(s.geo_id AS STRING))
            ORDER BY s.extract_timestamp_temp DESC
        ) AS rank_by_recency

    FROM 
        source_data AS s
)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE (Standardisée) **
    -- ************************************************************
    
    -- 1. URN (Première colonne)
    d._organization_entity_urn,
    
    -- 2. ROW_ID
    d.row_id,
    
    -- 3. extract_timestamp (Renommage)
    d.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. Dimension (Libellé enrichi)
    COALESCE(d.dim_geo_name_enriched, CAST(d.geo_id AS STRING)) AS dim_geo,
    
    -- 5. Dimension_ID
    d.geo_id AS dim_geo_id,
    
    -- 6. Day (format YYYY-MM-DD)
    d.day_format AS day,
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS (Métriques de Vues de Page) **
    -- ************************************************************
    
    d.all_desktop_page_views,
    d.all_mobile_page_views,
    d.all_page_views,
    d.about_page_views,
    d.careers_page_views,
    d.products_page_views,
    d.jobs_page_views,
    d.people_page_views,
    d.overview_page_views,
    d.life_at_page_views,
    d.insights_page_views,
    d.mobile_careers_page_views,
    d.mobile_overview_page_views,
    d.mobile_jobs_page_views,
    d.mobile_life_at_page_views,
    d.mobile_insights_page_views,
    d.mobile_products_page_views,
    d.mobile_about_page_views,
    d.mobile_people_page_views,
    d.desktop_insights_page_views,
    d.desktop_careers_page_views,
    d.desktop_life_at_page_views,
    d.desktop_jobs_page_views,
    d.desktop_people_page_views,
    d.desktop_about_page_views,
    d.desktop_overview_page_views,
    d.desktop_products_page_views
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1