/* 
    The purpose of this view is to display predictions per user with their scores
    Each row will be a user on a gameday, and columns are figures for each predictions game of this gameday
    It will be used by the Python program for output
*/
{{config(
    materialized="view"
)}}
-- we set a static number of game to 16, because we can't run Snowflake dynamically
-- the number of games in a gameday will always be lower.
-- when using it, the python program will cut the unnecessary games
{% set max_nb_games = 16 %}

-- we get the prediction
with predict_game AS (
    SELECT
        user.USER_NAME,
        game.GAME_MESSAGE_SHORT,
        ug.POINTS,
        game.RESULT,
        pg.PREDICT,
        CASE 
            WHEN pg.IS_BONUS_GAME = 1 THEN '*3' 
            ELSE '' 
        END AS BONUS,
        pg.SCORE_WIN,
        pg.SCORE_SHIFT,
        pg.AUTOMATIC_SCORE,
        gameday.GAMEDAY,
        season.SEASON_ID
        FROM
            {{ref('consumpted_predict_game')}} pg
        JOIN
            {{ref('consumpted_user')}} user            
            ON pg.USER_KEY = user.USER_KEY
        JOIN
            {{ref('consumpted_game')}} game
            ON game.GAME_KEY = pg.GAME_KEY
        JOIN
            {{ref('consumpted_gameday')}} gameday
            ON gameday.GAMEDAY_KEY = pg.GAMEDAY_KEY
        JOIN
            {{ref('consumpted_season')}} season
            ON season.SEASON_KEY = pg.SEASON_KEY 
        JOIN
            {{ref('consumpted_user_gameday')}} ug
            ON user.USER_KEY = ug.USER_KEY
            AND gameday.GAMEDAY_KEY = ug.GAMEDAY_KEY
)
-- we pivot the table to get users and gamedays in rows
SELECT
    USER_NAME,
    GAMEDAY,
    SEASON_ID,
    POINTS AS PT,
    {% for i in range(1, max_nb_games + 1) %}
        {% set i2 = "{:02d}".format(i) %}
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN BONUS END) AS G{{ i2 }}_BO,
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN RESULT END) AS G{{ i2 }}_RE,
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN PREDICT END) AS G{{ i2 }}_PR,
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN SCORE_WIN END) AS G{{ i2 }}_SW,
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN SCORE_SHIFT END) AS G{{ i2 }}_SD,
        MAX(CASE WHEN GAME_MESSAGE_SHORT = {{ i }} THEN AUTOMATIC_SCORE END) AS G{{ i2 }}_SA
        {% if not loop.last %},{% endif %}
    {% endfor %}
FROM 
    predict_game
GROUP BY
    USER_NAME,
    GAMEDAY,
    SEASON_ID,
    POINTS