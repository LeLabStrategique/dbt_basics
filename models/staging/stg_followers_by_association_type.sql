{{
    config(
        materialized='incremental',
        unique_key='row_id',
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        f.* EXCEPT(_fivetran_id), -- On exclut _fivetran_id car nous n'avons plus besoin de dedup_key

        -- Crée la clé analytique (row_id)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            f.association_type 
        ) AS row_id
        
    FROM 
        -- Source de faits
        {{ source('fivetran_linkedin', 'followers_by_association_type') }} AS f
        
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
    
    -- 3. Dimension (Type d'Association - pas besoin de COALESCE car c'est un STRING du source)
    association_type AS dim_split_association_type,
    
    -- 4. Métriques
    follower_counts_organic_follower_count,
    follower_counts_paid_follower_count,
    
    -- 5. URN de l'Organisation (Clé de la page)
    _organization_entity_urn
    
FROM 
    source_data