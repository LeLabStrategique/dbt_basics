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
        -- 1. CLÉS ET URNs
        t1.id AS ugc_post_urn, 
        t3._fivetran_synced, -- Dérivé de t3 (share_statistic)
        
        t1.author AS organization_entity, 
        
        -- Extraction de l'ID du post de l'URN (ID numérique)
        REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') AS dim_post_id_raw,
        
        -- 2. ATTRIBUTS DE PUBLICATION
        t1.created_time,
        t1.last_modified_time,
        t1.first_published_at,
        t1.commentary AS title, 
        t1.lifecycle_state,
        t1.visibility,
        t1.author, 
        
        -- 3. MÉTRIQUES DE STATISTIQUES (t3)
        t3.click_count,
        t3.comment_count,
        t3.engagement AS engagement_count, 
        t3.impression_count,
        t3.like_count,
        t3.share_count,
        
        -- Clé de jointure pour débug
        t2.share_statistic_id,
        
        -- Colonnes absentes dans la source share_statistic, mises à NULL
        CAST(NULL AS INT64) AS unique_impression_count,
        CAST(NULL AS INT64) AS video_view_count,
        CAST(NULL AS INT64) AS viral_impression_count,
        
        -- 4. COLONNES CALCULÉES
        TIMESTAMP(t3._fivetran_synced) AS extract_timestamp_temp, 
        CAST(DATE(TIMESTAMP(t3._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format

    FROM
        {{ source('fivetran_linkedin', 'ugc_post_history') }} AS t1
    
    LEFT JOIN
        {{ source('fivetran_linkedin', 'ugc_post_share_statistic') }} AS t2
    ON
        -- JOINTURE 1 : URN Extrait vers ID du post dans la table pivot
        REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') = CAST(t2.ugc_post_id AS STRING) 

    LEFT JOIN
        {{ source('fivetran_linkedin', 'share_statistic') }} AS t3
    ON
        -- CORRECTION DÉFINITIVE : Jointure de la clé du pivot (t2.share_statistic_id) à l'identifiant Fivetran de la ligne (t3._fivetran_id)
        t2.share_statistic_id = t3._fivetran_id  -- <<< CLÉ FIVETRAN INTERNE
    
    -- Exclure les lignes où l'extraction d'ID de post échoue
    WHERE REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') IS NOT NULL
    
    {% if is_incremental() %}
        AND t3._fivetran_synced > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

),

deduplication AS (
    SELECT
        s.* EXCEPT(organization_entity),
        s.organization_entity,
        
        -- Construction de la clé analytique (row_id)
        CONCAT(
            REPLACE(s.day_format, '-', ''), 
            '_', 
            s.dim_post_id_raw
        ) AS row_id,

        ROW_NUMBER() OVER(
            PARTITION BY 
                s.day_format, 
                s.dim_post_id_raw
            ORDER BY s.extract_timestamp_temp DESC 
        ) AS rank_by_recency

    FROM 
        source_data AS s
    -- FILTRE CRITIQUE : Garder uniquement les lignes qui ont trouvé une entrée dans 'share_statistic' (t3)
    WHERE 
        s.day_format IS NOT NULL 
        AND s._fivetran_synced IS NOT NULL
)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE (Standardisée) **
    -- ************************************************************
    
    d.organization_entity AS _organization_entity_urn,
    d.row_id,
    d.extract_timestamp_temp AS extract_timestamp,
    d.dim_post_id_raw AS dim_post_id,
    d.dim_post_id_raw AS dim_post_id_id,
    d.day_format AS day, 
    
    -- ************************************************************
    -- ** 2. ATTRIBUTS ET MÉTADONNÉES **
    -- ************************************************************
    
    d.ugc_post_urn,
    d.share_statistic_id,
    d.created_time,
    d.last_modified_time,
    d.first_published_at,
    d.title,
    d.lifecycle_state,
    d.visibility,
    d.author,
    
    -- ************************************************************
    -- ** 3. COLONNES DE FAITS (Métriques) **
    -- ************************************************************

    d.click_count,
    d.comment_count,
    d.engagement_count,
    d.impression_count,
    d.like_count,
    d.share_count,
    d.unique_impression_count,
    d.video_view_count,
    d.viral_impression_count
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1