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
        -- Jointure sur la table de dimension 'function'
        d.name AS function_name_enriched,
        
        -- Crée la clé analytique (row_id)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            -- Utilise le nom de la fonction, sinon l'ID brut
            COALESCE(d.name, CAST(f.function_id AS STRING)) 
        ) AS row_id,
        
        -- Clé de traçabilité Fivetran (Conservée ici pour le traçage, mais non sélectionnée dans le SELECT final)
        CONCAT(CAST(f._fivetran_id AS STRING), '§', CAST(TIMESTAMP(f._fivetran_synced) AS STRING)) AS dedup_key
        
    FROM 
        -- Source de faits
        {{ source('fivetran_linkedin', 'followers_by_function') }} AS f
        
    LEFT JOIN
        -- Source de dimension
        {{ source('fivetran_linkedin', 'function') }} AS d
        ON CAST(d.id AS STRING) = CAST(f.function_id AS STRING)
        
    -- FILTRE D'OPTIMISATION (Incrémentalité)
    {% if is_incremental() %}
        -- Ne scanner que les lignes plus récentes que le dernier traitement
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(stat_day_time) FROM {{ this }})
    {% endif %}

)

SELECT
    -- 1. Clé Analytique
    row_id,
    
    -- 2. Colonne de Temps
    TIMESTAMP(_fivetran_synced) AS stat_day_time,
    
    -- 3. Clé de Dimension
    function_id,
    
    -- 4. Libellé de Dimension enrichi
    COALESCE(function_name_enriched, CAST(function_id AS STRING)) AS dim_split_function,
    
    -- 5. Métriques
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    
    -- 6. URN de l'Organisation (Clé de la page)
    _organization_entity_urn
    
    -- La colonne dedup_key n'est plus présente ici, elle est retirée de la sortie finale.
    
FROM 
    source_data