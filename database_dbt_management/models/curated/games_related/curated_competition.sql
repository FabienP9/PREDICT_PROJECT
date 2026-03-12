/* 
    The purpose of this table is to extract all competitions we focus on
    Inputs:
        landing_competition: we get all distinct seasons
    Joins:
        curated_season: foreign key
    Primary Key:
        COMPETITION_KEY based on SEASON_ID and COMPETITION_ID
    Foreign key:
        SEASON_KEY: 1-n relationship with season
        local_time seed to calculate UPDATED_AT_LOCAL
    Filter:
        landing_competition is already filtered on competition which should be processed
    Materialization:
        incremental to avoid removing old competition already in
*/
{{config(
    tags=['init_compet'],
    materialized="incremental",
    unique_key = ['COMPETITION_KEY']
)}}

-- we get all competition from landing table
with compet as (
    SELECT DISTINCT
        compet.SEASON_ID,
        compet.COMPETITION_ID,
        compet.COMPETITION_SOURCE,
        compet.COMPETITION_LABEL,
        compet.IS_SAME_FOR_PREDICTCHAMP,
        compet.IS_FOR_RANK
    FROM
        {{source("LAND",'COMPET')}} compet
),
-- we calculate keys
compet_with_key as (
    SELECT
        MD5('COMPETITION' || '^^' || season.SEASON_ID || '^^' || competition.COMPETITION_ID) AS COMPETITION_KEY,
        season.SEASON_KEY,
        competition.COMPETITION_ID,
        competition.COMPETITION_SOURCE,
        competition.COMPETITION_LABEL,
        competition.IS_SAME_FOR_PREDICTCHAMP,
        competition.IS_FOR_RANK
    FROM
        compet competition
    LEFT JOIN
        {{ref('curated_season')}} season
        ON competition.SEASON_ID = season.SEASON_ID
)
SELECT
    final_compet.COMPETITION_KEY,
    final_compet.SEASON_KEY,
    final_compet.COMPETITION_ID,
    final_compet.COMPETITION_SOURCE,
    final_compet.COMPETITION_LABEL,
    final_compet.IS_SAME_FOR_PREDICTCHAMP,
    final_compet.IS_FOR_RANK,
    {{updated_at_fields()}}
FROM 
    compet_with_key final_compet
    {{updated_at_table_join_season('final_compet')}}