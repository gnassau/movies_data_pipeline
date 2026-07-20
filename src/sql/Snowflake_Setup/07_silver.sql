-- ============================================================
-- 07_silver.sql
-- Transformações e validações na camada Silver
-- Executar após o COPY INTO do 06_copy_into.sql
-- ============================================================

USE DATABASE MOVIES_DB;
USE SCHEMA SILVER;

-- ============================================================
-- VIEW: movies com campos derivados e validações
-- ============================================================

CREATE OR REPLACE VIEW SILVER.MOVIES_CLEAN AS
SELECT
    movie_id,
    title,
    release_date,
    YEAR(release_date)                                   AS release_year,
    MONTH(release_date)                                  AS release_month,
    budget,
    revenue,
    -- lucro bruto
    (revenue - budget)                                   AS profit,
    -- ROI: evita divisão por zero
    CASE
        WHEN budget > 0 THEN ROUND((revenue - budget) / budget * 100, 2)
        ELSE NULL
    END                                                  AS roi_pct,
    runtime,
    popularity,
    vote_average,
    vote_count,
    language,
    original_language,
    status,
    overview,
    adult,
    homepage,
    genres,
    year,
    ingestion_timestamp,
    loaded_at
FROM SILVER.MOVIES
WHERE
    movie_id    IS NOT NULL
    AND title   IS NOT NULL
    AND release_date IS NOT NULL
    AND status  = 'Released';

-- ============================================================
-- Checagem de qualidade básica (execute para validar)
-- ============================================================

-- Registros sem movie_id
-- SELECT COUNT(*) AS sem_movie_id FROM SILVER.MOVIES WHERE movie_id IS NULL;

-- Registros com revenue negativo
-- SELECT COUNT(*) AS revenue_negativo FROM SILVER.MOVIES WHERE revenue < 0;

-- Duplicatas
-- SELECT movie_id, COUNT(*) FROM SILVER.MOVIES GROUP BY 1 HAVING COUNT(*) > 1;
