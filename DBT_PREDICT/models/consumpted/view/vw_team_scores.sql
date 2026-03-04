/* 
    The purpose of this view is to display team with their scores.
    It will be used by the Python program to display the ranking of the prediction championship in the output_calculation message
*/
{{config(
    materialized="view"
)}}

SELECT
    season.SEASON_ID,
    team.TEAM_NAME,
    team.WIN,
    team.LOSS,
    team.PERC_WIN,
    team.POINTS_PRO,
    team.POINTS_AGAINST,
    team.POINTS_DIFF
FROM
    {{ref('consumpted_team')}} team
JOIN
    {{ref('consumpted_season')}} season
    ON team.SEASON_KEY = season.SEASON_KEY