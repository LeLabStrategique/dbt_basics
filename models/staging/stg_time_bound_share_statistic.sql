{{
    config(
        materialized='incremental',
        unique_key='DIM_SPLIT_day', 
        incremental_strategy='merge', 
        on_schema_change='fail'
    )
}}

WITH source_data AS (

    SELECT
        -- Exclure _fivetran_id, la colonne 'day' originale et organization_entity
        f.* EXCEPT(_fivetran_id, day, organization_entity), 
        
        -- Clé de temps (UTILISÉE POUR LA LOGIQUE)
        TIMESTAMP(f._fivetran_synced) AS Stat_Day_Time,
        
        -- DIM_SPLIT_day format YYYY-MM-DD
        CAST(DATE(f.day) AS STRING FORMAT 'YYYY-MM-DD') AS DIM_SPLIT_day,
        
        -- Renommer l'URN pour la traçabilité
        f.organization_entity AS organization_entity_raw,
        
        -- Construction du ROW_ID: YYYYMMDD(Stat_Day_Time) - YYYYMMDD(DIM_SPLIT_day)
        CONCAT(
            CAST(DATE(TIMESTAMP(f._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            CAST(DATE(f.day) AS STRING FORMAT 'YYYYMMDD')
        ) AS row_id
        
    FROM 
        {{ source('fivetran_linkedin', 'time_bound_share_statistic') }} AS f

    {% if is_incremental() %}
        -- Le filtre incrémental doit toujours utiliser Stat_Day_Time
        WHERE TIMESTAMP(f._fivetran_synced) > (SELECT MAX(Stat_Day_Time) FROM {{ this }})
    {% endif %}

)

SELECT
    -- 1. Clé analytique
    t.row_id,
    
    -- 2. Colonne de temps (Stat_Day_Time est exclu du SELECT final)
    
    -- 3. Dimension clé (YYYY-MM-DD)
    t.DIM_SPLIT_day,

    -- 4. Toutes les autres colonnes (métriques de partage)
    -- Stat_Day_Time est explicitement retiré du résultat final
    t.* EXCEPT(Stat_Day_Time, DIM_SPLIT_day, row_id, organization_entity_raw),
    
    -- 5. URN de l'Organisation (Renommage final)
    t.organization_entity_raw AS _organization_entity_urn

FROM 
    source_data AS t
QUALIFY ROW_NUMBER() OVER (
    -- La déduplication se base sur le temps de synchronisation
    PARTITION BY t.DIM_SPLIT_day
    ORDER BY t.Stat_Day_Time DESC 
) = 1