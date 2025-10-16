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
        
        -- Colonnes de FAITS (Métriques de Partage - Noms exacts confirmés)
        f.engagement,
        f.unique_impressions_count,
        f.share_count,
        f.share_mentions_count,
        f.click_count,
        f.like_count,
        f.impression_count,
        f.comment_count,
        f.comment_mentions_count,
        
        -- Colonnes temporelles calculées
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp, 
        CAST(f.day AS DATE) AS day_format,
        CAST(DATE(f.day) AS STRING FORMAT 'YYYY-MM-DD') AS day_format_string
        
    FROM 
        {{ source('fivetran_linkedin', 'time_bound_share_statistic') }} AS f

    {% if is_incremental() %}
        -- Utilise le nom de la colonne dans la table cible (extract_timestamp)
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        s.* EXCEPT(day_raw_timestamp), -- Sélectionne tout sauf la colonne brute pour la garder propre
        s.day_raw_timestamp, -- S'assure que le timestamp brut est réinclus
        
        -- Construction de la clé analytique (row_id)
        CONCAT(
            CAST(DATE(s.extract_timestamp_temp) AS STRING FORMAT 'YYYY-MM-DD'), 
            '_', 
            s.day_format_string
        ) AS row_id,

        -- Fenêtrage pour conserver la ligne la plus récente pour chaque jour d'agrégation
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
    
    -- 1. URN (Renommage final)
    d.organization_entity AS _organization_entity_urn,
    
    -- 2. ROW_ID
    d.row_id,
    
    -- 3. TIMESTAMP de la synchro
    d.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. DIM_DAY (TIMESTAMP brut du jour d'agrégation)
    d.day_raw_timestamp AS dim_day, 
    
    -- 5. DIM_DAY_ID
    CAST(NULL AS STRING) AS dim_day_id, 
    
    -- 6. DAY (Date d'agrégation au format YYYY-MM-DD)
    d.day_format AS day, 
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS (Métriques de Partage) **
    -- ************************************************************
    
    d.engagement,
    d.unique_impressions_count,
    d.share_count,
    d.share_mentions_count,
    d.click_count,
    d.like_count,
    d.impression_count,
    d.comment_count,
    d.comment_mentions_count
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1