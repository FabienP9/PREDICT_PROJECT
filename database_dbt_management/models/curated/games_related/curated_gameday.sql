/* 
    The purpose of this table is to extract all gamedays from games we focus on
    Inputs:
        adjusted_game (ephemeral): the extracted game via landing_game
        landing_output_need: to know on which gameday the run action is applied, if any
    Primary Key:
        GAMEDAY_KEY
    Foreign key:
        SEASON_KEY: 1-n relationship with season
        COMPETITION_KEY: 1-n relationship with competition
    Join:
        same model (this):
        - to get old date and time on same gamedays (same number of gamedays)
        - to change old gameday with action (only one)
        curated_season: foreign key for landing_output_need
    Filter:
        adjusted_game is filtered with games to process
        landing_output_need contains only the gameday to process
    Materialization:
        incremental to avoid removing old gamedays already in
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key=['GAMEDAY_KEY']
)}}
-- we get distinct games with their date and time from the ephemeral adjusted_game
with adjusted_game as (
    SELECT DISTINCT
        game.SEASON_KEY,
        game.COMPETITION_KEY,
        game.GAMEDAY_KEY,
        game.GAMEDAY,
        game.GAMEDAY_MESSAGE,
        game.DATE_GAME_LOCAL,
        game.TIME_GAME_LOCAL,
        game.DATE_GAME_UTC,
        game.TIME_GAME_UTC,
        game.SCORE_HOME,
        game.SCORE_AWAY,
        game.SCORE_HOME + game.SCORE_AWAY AS TOTAL_SCORE
    FROM
        {{ref('adjusted_game')}} game
),
-- we add 4 types of rank in order to select min and max at next cte
adjusted_game_sorted as (
    SELECT
        game_adj.SEASON_KEY,
        game_adj.COMPETITION_KEY,
        game_adj.GAMEDAY_KEY,
        game_adj.GAMEDAY,
        game_adj.GAMEDAY_MESSAGE,
        game_adj.DATE_GAME_LOCAL,
        game_adj.TIME_GAME_LOCAL,
        game_adj.DATE_GAME_UTC,
        game_adj.TIME_GAME_UTC,
        game_adj.SCORE_HOME,
        game_adj.SCORE_AWAY,
        -- by score ascending
        ROW_NUMBER() OVER (
                PARTITION BY game_adj.GAMEDAY_KEY
                ORDER BY game_adj.TOTAL_SCORE
            ) AS RANK_BY_TOTAL_SCORE_ASC,
        -- by score descending
        ROW_NUMBER() OVER (
                PARTITION BY game_adj.GAMEDAY_KEY
                ORDER BY game_adj.TOTAL_SCORE DESC
            ) AS RANK_BY_TOTAL_SCORE_DESC,
        -- by date, time ascending
        ROW_NUMBER() OVER (
                PARTITION BY game_adj.GAMEDAY_KEY
                ORDER BY CAST (game_adj.DATE_GAME_LOCAL AS DATE),
                         CAST (game_adj.TIME_GAME_LOCAL AS TIME)
            ) AS RANK_BY_DATETIME_ASC,
        -- by date, time descending
        ROW_NUMBER() OVER (
                PARTITION BY game_adj.GAMEDAY_KEY
                ORDER BY CAST (game_adj.DATE_GAME_LOCAL AS DATE) DESC,
                         CAST (game_adj.TIME_GAME_LOCAL AS TIME) DESC
            ) AS RANK_BY_DATETIME_DESC
    FROM
        adjusted_game game_adj
),
-- we select min (rank = 1 when asc) and max (rank = 1 when desc) 
-- to calculate begin and end date and time, and is (partially) played
gameday as (
    SELECT
        ags_min_score.SEASON_KEY,
        ags_min_score.COMPETITION_KEY,
        ags_min_score.GAMEDAY_KEY, 
        ags_min_score.GAMEDAY,
        ags_min_score.GAMEDAY_MESSAGE,
        -- if the min total score > 0 => all games have been played, the gameday has been played
        CASE 
            WHEN ags_min_score.SCORE_HOME + ags_min_score.SCORE_AWAY > 0 THEN 1
            ELSE 0
        END AS IS_PLAYED,
        -- if the max total score > 0 => at least one game has been played, the gameday has been partially played
        CASE 
            WHEN ags_max_score.SCORE_HOME + ags_max_score.SCORE_AWAY > 0 THEN 1
            ELSE 0
        END AS IS_PARTIALLY_PLAYED,
        ags_min_datetime.DATE_GAME_LOCAL AS BEGIN_DATE_LOCAL,
        ags_min_datetime.TIME_GAME_LOCAL AS BEGIN_TIME_LOCAL,
        ags_min_datetime.DATE_GAME_UTC AS BEGIN_DATE_UTC,
        ags_min_datetime.TIME_GAME_UTC AS BEGIN_TIME_UTC,
        ags_max_datetime.DATE_GAME_LOCAL AS END_DATE_LOCAL,
        ags_max_datetime.TIME_GAME_LOCAL AS END_TIME_LOCAL,
        ags_max_datetime.DATE_GAME_UTC AS END_DATE_UTC,
        ags_max_datetime.TIME_GAME_UTC AS END_TIME_UTC
    FROM
        adjusted_game_sorted ags_min_score
    LEFT JOIN
        adjusted_game_sorted ags_max_score
        ON ags_min_score.GAMEDAY_KEY = ags_max_score.GAMEDAY_KEY
        AND ags_max_score.RANK_BY_TOTAL_SCORE_DESC = 1
    LEFT JOIN
        adjusted_game_sorted ags_min_datetime
        ON ags_min_score.GAMEDAY_KEY = ags_min_datetime.GAMEDAY_KEY
        AND ags_min_datetime.RANK_BY_DATETIME_ASC = 1
    LEFT JOIN
        adjusted_game_sorted ags_max_datetime
        ON ags_min_score.GAMEDAY_KEY = ags_max_datetime.GAMEDAY_KEY
        AND ags_max_datetime.RANK_BY_DATETIME_DESC = 1
    WHERE
        ags_min_score.RANK_BY_TOTAL_SCORE_ASC = 1    
),
-- we get gameday processed in output_need
need as (
    SELECT
        need.SEASON_ID,
        need.GAMEDAY,
        need.IS_TO_INIT,
        need.IS_TO_CALCULATE,
        need.IS_TO_DELETE,
        need.IS_TO_RECALCULATE
    FROM
        {{source('LAND','NEED')}} need
),
-- we add keys
need_with_key as (
    SELECT
        season.SEASON_KEY,
        need2.GAMEDAY,
        need2.IS_TO_INIT,
        need2.IS_TO_CALCULATE,
        need2.IS_TO_DELETE,
        need2.IS_TO_RECALCULATE
    FROM
        need need2
    LEFT JOIN
        {{ref('curated_season')}} season
        ON season.SEASON_ID = need2.SEASON_ID
),
-- we merge gameday previously calculated with need to get which gameday is the action
gameday_with_need as (
    SELECT
        gameday.SEASON_KEY,
        gameday.COMPETITION_KEY,
        gameday.GAMEDAY_KEY, 
        gameday.GAMEDAY,
        gameday.GAMEDAY_MESSAGE,
        gameday.IS_PLAYED,
        gameday.IS_PARTIALLY_PLAYED,
        gameday.BEGIN_DATE_LOCAL,
        gameday.BEGIN_TIME_LOCAL,
        gameday.BEGIN_DATE_UTC,
        gameday.BEGIN_TIME_UTC,
        gameday.END_DATE_LOCAL,
        gameday.END_TIME_LOCAL,
        gameday.END_DATE_UTC,
        gameday.END_TIME_UTC,
        COALESCE(need_key.IS_TO_INIT,0) AS IS_TO_INIT,
        COALESCE(need_key.IS_TO_CALCULATE,0) AS IS_TO_CALCULATE,
        COALESCE(need_key.IS_TO_DELETE,0) AS IS_TO_DELETE,
        COALESCE(need_key.IS_TO_RECALCULATE,0) AS IS_TO_RECALCULATE
    FROM
        gameday 
    LEFT JOIN
        need_with_key need_key
        ON gameday.SEASON_KEY = need_key.SEASON_KEY
        AND gameday.GAMEDAY = need_key.GAMEDAY
)
-- if curated_gameday already exists we get it:
-- to recover date and time when gameday is already partially played or played:
-- to change gameday which were previously with action
{% if is_incremental() %}
,this_filtered as (
    SELECT
        this.SEASON_KEY,
        this.COMPETITION_KEY,
        this.GAMEDAY_KEY, 
        this.GAMEDAY,
        this.GAMEDAY_MESSAGE,
        this.IS_PLAYED,
        this.IS_PARTIALLY_PLAYED,
        this.BEGIN_DATE_LOCAL,
        this.END_DATE_LOCAL,
        this.BEGIN_TIME_LOCAL,
        this.END_TIME_LOCAL,
        this.BEGIN_DATE_UTC,
        this.END_DATE_UTC,
        this.BEGIN_TIME_UTC,
        this.END_TIME_UTC
    FROM
        {{this}} this
    LEFT JOIN
        gameday_with_need gameday_need
        ON this.GAMEDAY_KEY = gameday_need.GAMEDAY_KEY
    -- we keep gamedays...
    WHERE
        -- ...which are processed by previous cte, to possibly change dates and times
        gameday_need.GAMEDAY_KEY IS NOT NULL
    OR
        -- ...which were previously run action (we reset actions columns if it is not the same currently run)
        this.IS_TO_INIT + this.IS_TO_CALCULATE + this.IS_TO_DELETE + this.IS_TO_RECALCULATE > 0
),
-- we get last gameday cte (gameday_utc) and this_filtered in a full join. 
-- when date and time  are null: for (partially) played, it will be replaced by the current version 
gameday_with_this as (
    SELECT
        COALESCE(gameday2.SEASON_KEY,this.SEASON_KEY) AS SEASON_KEY,
        COALESCE(gameday2.COMPETITION_KEY,this.COMPETITION_KEY) AS COMPETITION_KEY,
        COALESCE(gameday2.GAMEDAY_KEY,this.GAMEDAY_KEY) AS GAMEDAY_KEY,
        COALESCE(gameday2.GAMEDAY,this.GAMEDAY) AS GAMEDAY,
        COALESCE(gameday2.GAMEDAY_MESSAGE,this.GAMEDAY_MESSAGE) AS GAMEDAY_MESSAGE,
        COALESCE(gameday2.IS_PLAYED,this.IS_PLAYED) AS IS_PLAYED,
        COALESCE(gameday2.IS_PARTIALLY_PLAYED,this.IS_PARTIALLY_PLAYED) AS IS_PARTIALLY_PLAYED,
        COALESCE(gameday2.BEGIN_DATE_LOCAL,this.BEGIN_DATE_LOCAL) AS BEGIN_DATE_LOCAL,
        COALESCE(gameday2.BEGIN_TIME_LOCAL,this.BEGIN_TIME_LOCAL) AS BEGIN_TIME_LOCAL,
        COALESCE(gameday2.BEGIN_DATE_UTC,this.BEGIN_DATE_UTC) AS BEGIN_DATE_UTC,
        COALESCE(gameday2.BEGIN_TIME_UTC,this.BEGIN_TIME_UTC) AS BEGIN_TIME_UTC,
        COALESCE(gameday2.END_DATE_LOCAL,this.END_DATE_LOCAL) AS END_DATE_LOCAL,
        COALESCE(gameday2.END_TIME_LOCAL,this.END_TIME_LOCAL) AS END_TIME_LOCAL,
        COALESCE(gameday2.END_DATE_UTC,this.END_DATE_UTC) AS END_DATE_UTC,
        COALESCE(gameday2.END_TIME_UTC,this.END_TIME_UTC) AS END_TIME_UTC,
        -- if it is the last run gameday, we set default to 0, unless we have it in gameday_with_need
        COALESCE(gameday2.IS_TO_INIT,0) AS IS_TO_INIT,
        COALESCE(gameday2.IS_TO_CALCULATE,0) AS IS_TO_CALCULATE,
        COALESCE(gameday2.IS_TO_DELETE,0) AS IS_TO_DELETE,
        COALESCE(gameday2.IS_TO_RECALCULATE,0) AS IS_TO_RECALCULATE
    FROM
        gameday_with_need gameday2
    FULL JOIN 
        this_filtered this
        ON this.GAMEDAY_KEY = gameday2.GAMEDAY_KEY
)
{% endif %}
-- we retrieve gamedays with all calculated info
SELECT
    final_gameday.GAMEDAY_KEY, 
    final_gameday.SEASON_KEY,
    final_gameday.COMPETITION_KEY,
    final_gameday.GAMEDAY,
    final_gameday.GAMEDAY_MESSAGE,
    final_gameday.IS_PLAYED,
    final_gameday.IS_PARTIALLY_PLAYED,
    final_gameday.BEGIN_DATE_LOCAL,
    final_gameday.BEGIN_TIME_LOCAL,
    final_gameday.END_DATE_LOCAL,
    final_gameday.END_TIME_LOCAL,
    final_gameday.BEGIN_DATE_UTC,
    final_gameday.BEGIN_TIME_UTC,
    final_gameday.END_DATE_UTC,
    final_gameday.END_TIME_UTC,
    final_gameday.IS_TO_INIT,
    final_gameday.IS_TO_CALCULATE,
    final_gameday.IS_TO_DELETE,
    final_gameday.IS_TO_RECALCULATE,
    {{updated_at_fields()}}
FROM 
    {%if is_incremental()%} gameday_with_this 
    {% else %} gameday_with_need
    {%endif%} final_gameday
    {{updated_at_table_join_season('final_gameday')}}