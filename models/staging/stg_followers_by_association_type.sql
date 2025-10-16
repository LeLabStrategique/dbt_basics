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
        f.association_type,
        f.follower_counts_organic_follower_count,
        f.follower_counts_paid_follower_count,
        f._fivetran_synced,
        
        -- Colonnes calculées/renommées
        f.association_type AS dim_association_type,
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp, 
        CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format
        
    FROM 
        {{ source('fivetran_linkedin', 'followers_by_association_type') }} AS f
        
    -- FILTRE D'OPTIMISATION (Incrémentalité)
    {% if is_incremental() %}
        -- Nous utilisons le nom de la colonne dans la table cible (extract_timestamp)
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
        s.dim_association_type 
    ) AS row_id,
    
    -- 3. extract_timestamp (Renommage depuis extract_timestamp_temp)
    s.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. Dimension (Libellé)
    s.dim_association_type,
    
    -- 5. Dimension_ID (Non disponible / Non pertinent)
    CAST(NULL AS STRING) AS dim_association_type_id,
    
    -- 6. Day (format YYYY-MM-DD)
    s.day_format AS day,
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS **
    -- ************************************************************
    
    s.follower_counts_organic_follower_count,
    s.follower_counts_paid_follower_count
    
FROM 
    source_data AS s