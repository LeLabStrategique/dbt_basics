{{
    config(
        materialized='incremental',
        unique_key='row_id', 
        incremental_strategy='merge',
        on_schema_change='fail'
    )
}}

WITH joined_data AS (
    SELECT
        t1.id AS ugc_post_urn,
        
        -- Clé de temps (Stat_Day_Time)
        TIMESTAMP(t3._fivetran_synced) AS Stat_Day_Time, 
        
        -- Extraction de l'ID de la publication (DIMENSION DE SPLIT)
        REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') AS DIM_SPLIT_postID,
        
        -- Construction du ROW_ID: YYYYMMDD(Stat_Day_Time) - Post ID
        CONCAT(
            CAST(DATE(TIMESTAMP(t3._fivetran_synced)) AS STRING FORMAT 'YYYYMMDD'), 
            ' - ', 
            REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$')
        ) AS row_id,

        -- Métriques de la statistique (t3)
        t3.* EXCEPT(_fivetran_id, _fivetran_synced),
        
        -- Attributs de la publication (t1)
        t1.* EXCEPT(
            id, 
            _fivetran_synced, 
            last_modified_time, 
            created_time, 
            first_published_at
        ),
        t1.created_time,
        t1.last_modified_time,
        t1.first_published_at,
        t2.share_statistic_id
        
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
    
    -- Correction de la syntaxe WHERE conditionnelle
    WHERE REGEXP_EXTRACT(t1.id, r':ugcPost:([^:]+)$') IS NOT NULL
    
    {% if is_incremental() %}
        AND t3._fivetran_synced > (SELECT MAX(Stat_Day_Time) FROM {{ this }})
    {% endif %}

)

SELECT
    -- 1. Clé de fusion
    t.row_id,
    
    -- 2. Dimension de split (Post ID)
    t.DIM_SPLIT_postID,

    -- 3. Colonne de date (YYYY-MM-DD)
    CAST(DATE(t.Stat_Day_Time) AS STRING FORMAT 'YYYY-MM-DD') AS day,
    
    -- 4. Stat_Day_Time est RÉTRO-INCLUS
    t.Stat_Day_Time,
    
    -- 5. Attributs et faits
    t.ugc_post_urn,
    t.share_statistic_id,
    t.created_time,
    t.last_modified_time,
    t.first_published_at,

    -- Toutes les autres colonnes
    t.* EXCEPT(
        row_id,
        Stat_Day_Time,
        DIM_SPLIT_postID,
        ugc_post_urn,
        share_statistic_id,
        created_time,
        last_modified_time,
        first_published_at
    )
    
FROM 
    joined_data AS t
QUALIFY ROW_NUMBER() OVER(
    PARTITION BY t.row_id
    ORDER BY t.Stat_Day_Time DESC 
) = 1