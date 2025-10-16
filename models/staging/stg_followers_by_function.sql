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
        -- Colonnes brutes nécessaires (pour la structure et les faits)
        f._organization_entity_urn,
        f.function_id, -- Clé de la dimension
        f.follower_counts_organic_follower_count,
        f.follower_counts_paid_follower_count,
        f._fivetran_synced, -- Pour l'incrémentalité et le timestamp
        
        -- Colonne de dimension enrichie (nom de la fonction)
        d.name AS dim_function_name_enriched,
        
        -- Colonnes calculées/renommées
        TIMESTAMP(f._fivetran_synced) AS stat_day_time, 
        CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format
        
    FROM 
        -- Source de faits
        {{ source('fivetran_linkedin', 'followers_by_function') }} AS f
        
    LEFT JOIN
        -- Source de dimension (pour enrichir l'ID)
        {{ source('fivetran_linkedin', 'function') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.function_id AS STRING)
        
    -- FILTRE D'OPTIMISATION (Incrémentalité)
    {% if is_incremental() %}
        -- Utilisation de 'extract_timestamp' qui sera le nom de la colonne dans la table cible
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(extract_timestamp) FROM {{ this }})
    {% endif %}

)

SELECT
    
    -- ************************************************************
    -- ** 1. STRUCTURE DE COLONNES DEMANDÉE **
    -- ************************************************************
    
    -- 1. URN (Première colonne)
    s._organization_entity_urn,
    
    -- 2. ROW_ID (day_format _ dimension_label)
    CONCAT(
        s.day_format, 
        '_', 
        COALESCE(s.dim_function_name_enriched, CAST(s.function_id AS STRING)) 
    ) AS row_id,
    
    -- 3. extract_timestamp (Renommage de stat_day_time)
    s.stat_day_time AS extract_timestamp,
    
    -- 4. Dimension (Libellé enrichi)
    COALESCE(s.dim_function_name_enriched, CAST(s.function_id AS STRING)) AS dim_function,
    
    -- 5. Dimension_ID
    s.function_id AS dim_function_id,
    
    -- 6. Day (format YYYY-MM-DD)
    s.day_format AS day,
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS **
    -- ************************************************************
    
    s.follower_counts_organic_follower_count,
    s.follower_counts_paid_follower_count -- <-- CORRECTION APPLIQUÉE : PAS DE VIRGULE ICI
    
FROM 
    source_data AS s