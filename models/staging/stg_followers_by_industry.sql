{{
    config(
        materialized='incremental',
        unique_key='row_id',
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        f.*,
        -- Jointure sur la colonne 'name' de la dimension 'industry'
        d.name AS industry_name_enriched,
        
        -- Crée la clé analytique (row_id)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            -- Utilise la colonne 'name' pour le COALESCE, sinon la clé f.industry_id
            COALESCE(d.name, CAST(f.industry_id AS STRING)) 
        ) AS row_id,
        
        -- Clé de traçabilité Fivetran
        CONCAT(CAST(f._fivetran_id AS STRING), '§', CAST(TIMESTAMP(f._fivetran_synced) AS STRING)) AS dedup_key
        
    FROM 
        -- Source de faits
        {{ source('fivetran_linkedin', 'followers_by_industry') }} AS f
        
    LEFT JOIN
        -- Source de dimension
        {{ source('fivetran_linkedin', 'industry') }} AS d
        -- Jointure basée sur l'ID de l'industrie
        ON CAST(d.id AS STRING) = CAST(f.industry_id AS STRING)
        
    -- FILTRE D'OPTIMISATION (Incrémentalité)
    {% if is_incremental() %}
        -- Ne scanner que les lignes plus récentes que le dernier traitement
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(stat_day_time) FROM {{ this }})
    {% endif %}

)

SELECT
    -- Colonnes finales
    TIMESTAMP(_fivetran_synced) AS stat_day_time,
    row_id,
    dedup_key,
    
    -- Colonne de l'Industrie enrichie (dim_split)
    COALESCE(industry_name_enriched, CAST(industry_id AS STRING)) AS dim_split_industry,
    
    -- Métriques et autres clés
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    _organization_entity_urn,
    industry_id
    
FROM 
    source_data