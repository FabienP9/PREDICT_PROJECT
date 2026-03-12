/*  The purpose of this table is to calculate teams results for the prediction championship
    Inputs:
        curated_team: the curated team
    Primary Key:
        TEAM_KEY from curated
    Foreign Key:
        SEASON_KEY: from curated
    Filter:
        Only teams related with season we processed from landing_season
    Materialization:
        incremental to avoid removing old calculations */

{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['TEAM_KEY']
)}}
-- we get last run timestamp if exists
{% if is_incremental() %}
    {% set results = run_query("SELECT MAX(UPDATED_AT_UTC) as max_updated_at FROM " ~ this) %}
    {% set max_updated_at = results.columns[0].values()[0] if results.columns[0].values() else none %}
{% endif %}
-- we get only teams from updated games
with team_from_game as (
    
        SELECT DISTINCT 
            game.TEAM_HOME_KEY AS TEAM_KEY 
        FROM
            {{ref('consumpted_game')}} game
        {% if is_incremental() and max_updated_at is not none %}
            WHERE game.UPDATED_AT_UTC > '{{ max_updated_at }}'
        {% endif %}
    UNION 
        SELECT DISTINCT 
            game.TEAM_AWAY_KEY AS TEAM_KEY 
        FROM
            {{ref('consumpted_game')}} game
        {% if is_incremental() and max_updated_at is not none %}
            WHERE game.UPDATED_AT_UTC > '{{ max_updated_at }}'
        {% endif %}
),    
team as (
    SELECT
        team.TEAM_KEY,
        team.TEAM_NAME,
        team.SEASON_KEY
    FROM
        {{ref('curated_team')}} team
    JOIN
        team_from_game tg
        ON team.TEAM_KEY = tg.TEAM_KEY
),
-- we calculate the number of win and loss and total points
calculation as (
    SELECT
        team2.TEAM_KEY,
        team2.TEAM_NAME,
        team2.SEASON_KEY,
        COALESCE(SUM(CASE 
                -- wins at home
                WHEN team2.TEAM_KEY = gp.TEAM_HOME_KEY AND gp.WINNER=1 AND gp.IS_FOR_RANK=1 THEN 1
                -- wins away
                WHEN team2.TEAM_KEY = gp.TEAM_AWAY_KEY AND gp.WINNER=2 AND gp.IS_FOR_RANK=1 THEN 1
                ELSE 0 END),0)
        AS WIN,
        COALESCE(SUM(CASE 
                -- loss at home
                WHEN team2.TEAM_KEY = gp.TEAM_HOME_KEY AND gp.WINNER=2 AND gp.IS_FOR_RANK=1 THEN 1
                -- loss away
                WHEN team2.TEAM_KEY = gp.TEAM_AWAY_KEY AND gp.WINNER=1 AND gp.IS_FOR_RANK=1 THEN 1
                ELSE 0 END),0)
        AS LOSS,
        COALESCE(SUM(CASE 
                -- points by the team at home
                WHEN team2.TEAM_KEY = gp.TEAM_HOME_KEY AND gp.IS_FOR_RANK=1 THEN gp.POINTS_HOME
                -- points by the team away
                WHEN team2.TEAM_KEY = gp.TEAM_AWAY_KEY AND gp.IS_FOR_RANK=1 THEN gp.POINTS_AWAY
                ELSE 0 END),0)
        AS POINTS_PRO,
        COALESCE(SUM(CASE 
                -- points by the opposite team when at home
                WHEN team2.TEAM_KEY = gp.TEAM_HOME_KEY AND gp.IS_FOR_RANK=1 THEN gp.POINTS_AWAY
                -- points by the opposite team when away
                WHEN team2.TEAM_KEY = gp.TEAM_AWAY_KEY AND gp.IS_FOR_RANK=1 THEN gp.POINTS_HOME
                ELSE 0 END),0)
        AS POINTS_AGAINST
    FROM
        team team2
    LEFT JOIN
        {{ref('consumpted_game_predictchamp')}} gp
        ON team2.TEAM_KEY = gp.TEAM_HOME_KEY
        OR team2.TEAM_KEY = gp.TEAM_AWAY_KEY 
    GROUP BY
        team2.TEAM_KEY,
        team2.TEAM_NAME,
        team2.SEASON_KEY
),
--we retrieve teams with more features: percentage of win and points difference
final_team as (
    SELECT
        calculation.TEAM_KEY,
        calculation.TEAM_NAME,
        calculation.SEASON_KEY,
        calculation.WIN,
        calculation.LOSS,
        CASE
            WHEN calculation.WIN+calculation.LOSS = 0 THEN 50
            ELSE CAST(100*calculation.WIN / (calculation.WIN+calculation.LOSS) AS DECIMAL(5,2))
        END AS PERC_WIN,  
        calculation.POINTS_PRO,        
        calculation.POINTS_AGAINST,
        calculation.POINTS_PRO - calculation.POINTS_AGAINST AS POINTS_DIFF  
    FROM
        calculation 
)
SELECT
    final_team.TEAM_KEY,
    final_team.SEASON_KEY,
    final_team.TEAM_NAME,
    final_team.WIN,
    final_team.LOSS,
    final_team.PERC_WIN,
    final_team.POINTS_PRO,
    final_team.POINTS_AGAINST,
    final_team.POINTS_DIFF,
    {{updated_at_fields()}}
FROM 
    final_team
    {{updated_at_table_join_season('final_team')}}