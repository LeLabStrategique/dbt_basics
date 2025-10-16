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
        t1.id AS ugc_post_urn, -- URN de la publication
        t3._fivetran_synced, -- Timestamp de la synchro pour la logique incrémentale
        
        -- URN de l'Organisation (CORRIGÉ : utilise 'author' au lieu d'une colonne inexistante)
        t1.author AS organization_entity, 
        
        -- Extraction de l'ID de la publication (DIMENSION)
        REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') AS dim_post_id_raw,
        
        -- 2. ATTRIBUTS DE PUBLICATION (t1) - Basé sur ugc_post_history
        t1.created_time,
        t1.last_modified_time,
        t1.first_published_at,
        t1.commentary AS title, -- Assumé que 'commentary' est le titre/contenu
        t1.lifecycle_state,
        t1.visibility,
        t1.author, 
        
        -- Attributs détaillés (laissez-les même si non utilisés dans le SELECT final, pour référence)
        t1.ad_context_dsc_ad_account,
        t1.ad_context_dsc_ad_type,
        t1.ad_context_dsc_name,
        t1.ad_context_dsc_status,
        t1.ad_context_is_dsc,
        t1.distribution_feed_distribution,
        t1.lifecycle_state_info_content_status,
        t1.lifecycle_state_info_is_edited_by_author,
        t1.lifecycle_state_info_review_status,
        t1.response_context_parent,
        t1.response_context_root,
        t1.container_entity,
        t1.content_landing_page,
        t1.content_call_to_action_label,
        t1.is_reshare_disabled_by_author,
        
        -- 3. CLÉS DE JOINTURE (t2)
        t2.share_statistic_id,
        
        -- 4. MÉTRIQUES DE STATISTIQUES (t3) - Basé sur share_statistic
        t3.click_count,
        t3.comment_count,
        -- CORRECTION DE LA DERNIÈRE ERREUR : 'engagement' au lieu de 'engagement_count'
        t3.engagement AS engagement_count, 
        t3.impression_count,
        t3.like_count,
        t3.share_count,
        
        -- CORRECTION DES COLONNES MANQUANTES : Mise à NULL selon le schéma
        CAST(NULL AS INT64) AS unique_impression_count,
        CAST(NULL AS INT64) AS video_view_count,
        CAST(NULL AS INT64) AS viral_impression_count,
        
        -- 5. COLONNES CALCULÉES
        TIMESTAMP(t3._fivetran_synced) AS extract_timestamp_temp, 
        CAST(DATE(TIMESTAMP(t3._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format

    FROM
        {{ source('fivetran_linkedin', 'ugc_post_history') }} AS t1
    
    LEFT JOIN
        {{ source('fivetran_linkedin', 'ugc_post_share_statistic') }} AS t2
    ON
        REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') = CAST(t2.ugc_post_id AS STRING) 

    LEFT JOIN
        {{ source('fivetran_linkedin', 'share_statistic') }} AS t3
    ON
        t2.share_statistic_id = t3._fivetran_id
    
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
)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE (Standardisée) **
    -- ************************************************************
    
    -- 1. URN Organisation
    d.organization_entity AS _organization_entity_urn,
    
    -- 2. ROW_ID (Clé de fusion et d'unicité)
    d.row_id,
    
    -- 3. extract_timestamp (Timestamp de la donnée)
    d.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. Dimension (Post ID)
    d.dim_post_id_raw AS dim_post_id,
    
    -- 5. Dimension ID (Le Post ID est l'ID)
    d.dim_post_id_raw AS dim_post_id_id,
    
    -- 6. Day (Date de synchronisation, format YYYY-MM-DD)
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
    -- Métriques à NULL (non trouvées dans share_statistic)
    d.unique_impression_count,
    d.video_view_count,
    d.viral_impression_count
    
FROM 
    deduplication AS d
WHERE
    d.rank_by_recency = 1