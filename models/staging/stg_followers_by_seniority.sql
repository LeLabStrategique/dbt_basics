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
        f.seniority_id, -- Clé de la dimension
        f.follower_counts_organic_follower_count,
        f.follower_counts_paid_follower_count,
        f._fivetran_synced, -- Pour l'incrémentalité et le timestamp
        
        -- Colonne de dimension enrichie (nom de l'ancienneté)
        d.name AS dim_seniority_name_enriched,
        
        -- Colonnes calculées/renommées
        TIMESTAMP(f._fivetran_synced) AS extract_timestamp_temp, -- Utilisé comme source pour 'extract_timestamp'
        CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYY-MM-DD') AS day_format
        
    FROM 
        -- Source de faits
        {{ source('fivetran_linkedin', 'followers_by_seniority') }} AS f
        
    LEFT JOIN
        -- Source de dimension (pour enrichir l'ID d'ancienneté)
        {{ source('fivetran_linkedin', 'seniority') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.seniority_id AS STRING)
        
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
        -- Utilisation du nom enrichi, sinon l'ID brut
        COALESCE(s.dim_seniority_name_enriched, CAST(s.seniority_id AS STRING)) 
    ) AS row_id,
    
    -- 3. extract_timestamp (Renommage depuis extract_timestamp_temp)
    s.extract_timestamp_temp AS extract_timestamp,
    
    -- 4. Dimension (Libellé enrichi)
    COALESCE(s.dim_seniority_name_enriched, CAST(s.seniority_id AS STRING)) AS dim_seniority,
    
    -- 5. Dimension_ID
    s.seniority_id AS dim_seniority_id,
    
    -- 6. Day (format YYYY-MM-DD)
    s.day_format AS day,
    
    -- ************************************************************
    -- ** 2. COLONNES DE FAITS **
    -- ************************************************************
    
    s.follower_counts_organic_follower_count,
    s.follower_counts_paid_follower_count
    
FROM 
    source_data AS s