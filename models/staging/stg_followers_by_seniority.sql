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
        -- Jointure sur la table de dimension 'seniority'
        d.name AS seniority_name_enriched,
        
        -- Crée la clé analytique (row_id)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            -- Utilise l'ID de séniorité du champ original 'seniority' s'il existe
            COALESCE(d.name, CAST(f.seniority_id AS STRING)) 
        ) AS row_id,
        
        -- Clé de traçabilité Fivetran
        CONCAT(CAST(f._fivetran_id AS STRING), '§', CAST(TIMESTAMP(f._fivetran_synced) AS STRING)) AS dedup_key
        
    FROM 
        -- RÉFÉRENCE CORRIGÉE : Utilise le nom de source déclaré 'fivetran_linkedin' 
        {{ source('fivetran_linkedin', 'followers_by_seniority') }} AS f
        
    LEFT JOIN
        -- RÉFÉRENCE CORRIGÉE : Utilise le nom de source déclaré 'fivetran_linkedin'
        {{ source('fivetran_linkedin', 'seniority') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.seniority_id AS STRING)
        
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
    
    -- Colonne de la Séniorité enrichie (dim_split)
    COALESCE(seniority_name_enriched, CAST(seniority_id AS STRING)) AS dim_split_seniority,
    
    -- Autres colonnes du Mart
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    _organization_entity_urn,
    seniority_id
    
    -- NOTE: Ajoutez ici explicitement toutes les autres colonnes brutes que vous voulez conserver.
    
FROM 
    source_data