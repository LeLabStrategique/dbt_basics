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
        -- Jointure sur la colonne 'value' de la dimension 'geo'
        d.value AS geo_country_name_enriched,
        
        -- Crée la clé analytique (row_id)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            -- Utilise la colonne 'value' pour le COALESCE, sinon la clé f.geo
            COALESCE(d.value, CAST(f.geo AS STRING)) 
        ) AS row_id,
        
        -- Clé de traçabilité Fivetran
        CONCAT(CAST(f._fivetran_id AS STRING), '§', CAST(TIMESTAMP(f._fivetran_synced) AS STRING)) AS dedup_key
        
    FROM 
        -- Source de faits : followers_by_geo_country
        {{ source('fivetran_linkedin', 'followers_by_geo_country') }} AS f
        
    LEFT JOIN
        -- Source de dimension : geo
        {{ source('fivetran_linkedin', 'geo') }} AS d
        -- Jointure correcte : d.id vers f.geo
        ON CAST(d.id AS STRING) = CAST(f.geo AS STRING)
        
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
    
    -- Colonne de la Géolocalisation enrichie (dim_split)
    COALESCE(geo_country_name_enriched, CAST(geo AS STRING)) AS dim_split_geo_country,
    
    -- Métriques et autres clés
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    _organization_entity_urn,
    geo
    
FROM 
    source_data