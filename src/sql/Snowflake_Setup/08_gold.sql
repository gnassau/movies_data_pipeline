-- ============================================================
-- 08_gold.sql
-- Cria as tabelas e views analíticas da camada Gold
-- Fonte: SILVER.MOVIES_CLEAN
-- ============================================================

USE DATABASE MOVIES_DB;
USE SCHEMA GOLD;

-- ============================================================
-- Tabela base Gold: snapshot analítico da silver
-- ============================================================

CREATE OR REPLACE TABLE GOLD.MOVIES AS
SELECT
    movie_id,
    title,
    release_date,
    release_year,
    release_month,
    budget,
    revenue,
    profit,
    roi_pct,
    runtime,
    popularity,
    vote_average,
    vote_count,
    language,
    original_language,
    status,
    overview,
    adult,
    genres,
    year,
    ingestion_timestamp
FROM SILVER.MOVIES_CLEAN;

-- ============================================================
-- Tabela explodida de gêneros (para análises por gênero)
-- ============================================================

CREATE OR REPLACE TABLE GOLD.MOVIE_GENRES AS
SELECT
    m.movie_id,
    m.title,
    m.release_year,
    m.vote_average,
    m.vote_count,
    m.popularity,
    m.revenue,
    m.budget,
    m.profit,
    g.value::VARCHAR AS genre
FROM SILVER.MOVIES_CLEAN m,
     LATERAL FLATTEN(input => m.genres) g;

-- ============================================================
-- VIEW: Best Movies — maior rating com volume mínimo de votos
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_BEST_MOVIES AS
SELECT
    title,
    vote_average,
    vote_count,
    popularity,
    release_year
FROM GOLD.MOVIES
WHERE vote_count > 1000
ORDER BY vote_average DESC;

-- ============================================================
-- VIEW: Most Profitable Movies
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_MOST_PROFITABLE AS
SELECT
    title,
    revenue,
    budget,
    profit,
    roi_pct,
    release_year
FROM GOLD.MOVIES
WHERE budget > 0
ORDER BY profit DESC;

-- ============================================================
-- VIEW: High Engagement Movies
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_HIGH_ENGAGEMENT AS
SELECT
    title,
    popularity,
    vote_average,
    vote_count,
    release_year
FROM GOLD.MOVIES
WHERE vote_count > 100
ORDER BY popularity DESC;

-- ============================================================
-- VIEW: Movies by Year
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_MOVIES_BY_YEAR AS
SELECT
    release_year                    AS year,
    COUNT(*)                        AS total_movies,
    ROUND(AVG(vote_average), 2)     AS avg_rating,
    SUM(revenue)                    AS total_revenue,
    SUM(budget)                     AS total_budget,
    SUM(profit)                     AS total_profit
FROM GOLD.MOVIES
GROUP BY release_year
ORDER BY release_year;

-- ============================================================
-- VIEW: Genres Performance
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_GENRES_PERFORMANCE AS
SELECT
    genre,
    COUNT(*)                        AS total_movies,
    ROUND(AVG(vote_average), 2)     AS avg_rating,
    ROUND(AVG(popularity), 2)       AS avg_popularity,
    SUM(revenue)                    AS total_revenue,
    SUM(profit)                     AS total_profit
FROM GOLD.MOVIE_GENRES
GROUP BY genre
ORDER BY avg_rating DESC;

-- ============================================================
-- VIEW: General Performance — KPIs globais
-- ============================================================

CREATE OR REPLACE VIEW GOLD.V_GENERAL_PERFORMANCE AS
SELECT
    COUNT(*)                        AS total_movies,
    ROUND(AVG(vote_average), 2)     AS avg_rating,
    SUM(revenue)                    AS total_revenue,
    SUM(budget)                     AS total_budget,
    SUM(profit)                     AS total_profit,
    ROUND(AVG(profit), 2)           AS avg_profit,
    ROUND(AVG(roi_pct), 2)          AS avg_roi_pct
FROM GOLD.MOVIES;
