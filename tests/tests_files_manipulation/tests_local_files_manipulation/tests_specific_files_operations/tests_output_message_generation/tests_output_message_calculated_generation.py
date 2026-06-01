'''
This tests file concern all functions in the output_message_calculated_generation module.
It units test the happy path for each function
'''
from unittest.mock import patch
from pandas.testing import assert_frame_equal
import pandas as pd

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_calculated_generation

def test_get_games_result(read_yml_as_serie, read_csv, read_txt):

    # this test the function get_games_result   
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_games = read_csv("q_vw_game_query.csv")

    with patch.object(output_message_calculated_generation, 'snowflake_execute', return_value=mock_df_games):
        s, count = output_message_calculated_generation.get_games_result(sr_snowflake_account_connect, sr_gameday_output_calculate)
        s_expected = read_txt("output_message_calculated_games_result.txt")
        
        assert count == 2
        assert s == s_expected

def test_get_scores_detailed(read_yml_as_serie, read_csv):

    # this test the function get_scores_detailed
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_predict_games = read_csv("q_vw_predict_game_query.csv",keep_default_na=False,na_filter=False)
    df_predict_games_expected = read_csv("output_message_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_predict_games):
        df_result, n_users = output_message_calculated_generation.get_scores_detailed(sr_snowflake_account_connect, sr_gameday_output_calculate)
        
        # Make column names match
        top = df_predict_games_expected.columns.get_level_values(0)
        sub = df_predict_games_expected.columns.get_level_values(1)
        top = pd.Series(top).where(~top.str.contains("Unnamed"), None).ffill()
        sub = pd.Series(sub).where(~sub.str.contains("Unnamed"), '').ffill()
        df_predict_games_expected.columns = pd.MultiIndex.from_arrays([top, sub])
        df_predict_games_expected.columns.names = df_result.columns.names

        # Replace 'Unnamed' entries by forward-filling the previous non-unnamed label
        assert_frame_equal(df_result.reset_index(drop=True), df_predict_games_expected.reset_index(drop=True))
        assert n_users == 2

def test_get_scores_global(read_csv):

    # this test the function get_scores_global
    df_userscores_global = read_csv("q_vw_user_scores_global_query.csv")
    mock_df_rank = read_csv("q_vw_user_scores_global_query_ranked.csv")
    df_expected = read_csv("output_message_calculated_scores_global.csv")
    
    with patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_rank):
        df_result,nb_result = output_message_calculated_generation.get_scores_global(df_userscores_global)

    assert_frame_equal(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))
    assert nb_result == 2

def test_get_scores_gameday(read_yml_as_serie, read_csv):
    
    # this test the function get_scores_gameday
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = read_csv("q_vw_user_scores_gameday_query.csv")
    mock_df_userscores_gameday_ranked = read_csv("q_vw_user_scores_gameday_query.csv")
    df_expected = read_csv("output_message_calculated_scores_gameday.csv")
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            df_result, nb_result = output_message_calculated_generation.get_scores_gameday(sr_snowflake_account_connect, sr_gameday_output_calculate)
            assert_frame_equal(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))
            assert nb_result == 2

def test_get_scores_average(read_csv, read_txt):

    # this test the function get_scores_average
    nb_prediction = 67
    df_userscores_global = read_csv("q_vw_user_scores_global_query.csv")
    mock_df_rank = read_csv("q_vw_user_scores_global_query_average_ranked.csv")
    expected_str = read_txt("output_message_calculated_scores_average.txt")
    
    with patch.object(output_message_calculated_generation.output, 'calculate_and_display_rank', return_value= mock_df_rank):
        s, count, nb_min = output_message_calculated_generation.get_scores_average(nb_prediction, df_userscores_global)
        assert s == expected_str
        assert nb_min == 33
        assert count == 1

def test_get_predictchamp_result(read_yml_as_serie,read_csv, read_txt):
    
    # this test the function get_predictchamp_result
    df_gamepredictchamp = read_csv("q_vw_game_predictchamp_query.csv")
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_detail = read_csv("q_vw_game_predictchamp_details_query.csv")
    expected_str = read_txt("output_message_calculated_predictchamp_result.txt")

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_detail):
        result = output_message_calculated_generation.get_predictchamp_result(df_gamepredictchamp, sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert result == expected_str

def test_get_predictchamp_ranking(read_yml_as_serie, read_csv):
    
    # this test the function get_predictchamp_ranking
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_teamscores = read_csv("q_vw_team_scores_query.csv")
    mock_df_rank = read_csv("q_vw_team_scores_query_ranked.csv")
    expected_result = read_csv("output_message_calculated_predictchamp_rank.csv")

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_teamscores):
        with patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_rank) as mock_rank:
            result_df = output_message_calculated_generation.get_predictchamp_ranking(sr_snowflake_account_connect, sr_gameday_output_calculate)

            mock_rank.assert_called_once_with(mock_df_teamscores, 'RANK')
            pd.testing.assert_frame_equal(result_df, expected_result)

def test_get_correction(read_yml_as_serie, read_csv, read_txt):
    
    # this test the function get_correction
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_correction = read_csv("q_vw_correction.csv")
    expected_str = read_txt("output_message_calculated_correction.txt")

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_correction):
        result_str, result_count = output_message_calculated_generation.get_correction(sr_snowflake_account_connect, sr_gameday_output_calculate)

        assert result_str == expected_str
        assert result_count == 2

def test_get_list_gameday(read_csv, read_txt):
    
    # this test the function get_list_gameday
    df_list_gameday = read_csv("q_vw_gameday_calculated_query.csv")
    expected_str = read_txt("output_message_calculated_list_gameday.txt")
    str_list_gameday = output_message_calculated_generation.get_list_gameday(df_list_gameday)
    assert str_list_gameday == expected_str

def test_get_mvp_month_race_figure(read_yml_as_serie, read_csv, read_txt):
    
    # this test the function get_predictchamp_ranking
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = read_csv("q_vw_user_scores_gameday_mvp_query.csv",quotechar='"',keep_default_na=False)
    expected_str = read_txt("output_message_calculated_mvp_race_figures.txt")

    # mock snowflake_execute to return df_teamscores
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_message_calculated_generation.get_mvp_month_race_figure(sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert gameday_month == "__L__MONTH_01__L__"
        assert count == 3
        assert list_user == expected_str

def test_list_mvp_month_race_gameday(read_yml_as_serie, read_csv, read_txt):
    
    # this test the function list_mvp_month_race_gameday
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    
    mock_df_list_gameday = read_csv("q_vw_gameday_calculated_query.csv")
    expected_str = read_txt("output_message_calculated_list_gameday_with_predict.txt")
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_list_gameday):
        str_list_gameday = output_message_calculated_generation.list_mvp_month_race_gameday(sr_snowflake_account_connect,sr_gameday_output_calculate)
        assert str_list_gameday.split() == expected_str.split()

def test_get_mvp_compet_race_figure(read_yml_as_serie, read_csv, read_txt):
    
    # this test the function get_predictchamp_ranking
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_compet_mvp = read_csv("q_vw_user_scores_gameday_mvp_query.csv",quotechar='"',keep_default_na=False)
    expected_str = read_txt("output_message_calculated_mvp_race_figures.txt")

    # mock snowflake_execute to return df_teamscores
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_compet_mvp):
        (list_user, count) = output_message_calculated_generation.get_mvp_compet_race_figure(sr_snowflake_account_connect, sr_gameday_output_calculate)
        
        assert count == 3
        assert list_user == expected_str

def test_list_mvp_compet_race_gameday(read_yml_as_serie,read_csv, read_txt):
    
    # this test the function list_mvp_compet_race_gameday
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    
    mock_df_list_gameday = read_csv("q_vw_gameday_calculated_query.csv")
    expected_str = read_txt("output_message_calculated_list_gameday_with_predict.txt")
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_list_gameday):
        str_list_gameday = output_message_calculated_generation.list_mvp_compet_race_gameday(sr_snowflake_account_connect,sr_gameday_output_calculate)
        assert str_list_gameday.split() == expected_str.split()

def test_get_parameters(read_yml_as_serie, read_csv, read_txt):

    # this test the function get_parameters mocking all functions
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]

    mock_str_games_result = read_txt("output_message_calculated_games_result.txt")
    mock_df_predict_games = read_csv("output_message_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_df_userscores_global = read_csv("q_vw_user_scores_global_query.csv")
    mock_df_scores_global = read_csv("output_message_calculated_scores_global.csv")
    mock_df_list_gameday = read_csv("q_vw_gameday_calculated_query.csv")
    mock_str_scores_average = read_txt("output_message_calculated_scores_average.txt")
    mock_df_scores_gameday = read_csv("output_message_calculated_scores_gameday.csv")
    mock_str_list_gameday = read_txt("output_message_calculated_list_gameday.txt")
    mock_df_gamepredictchamp = read_csv("q_vw_game_predictchamp_query.csv")
    mock_str_predictchamp_result = read_txt("output_message_calculated_predictchamp_result.txt")
    mock_df_predictchamp_rank = read_csv("output_message_calculated_predictchamp_rank.csv")
    mock_str_correction = read_txt("output_message_calculated_correction.txt")
    mock_str_mvp_month = read_txt("output_message_calculated_mvp_race_figures.txt")
    mock_str_mvp_month_gameday = read_txt("output_message_calculated_list_gameday_with_predict.txt")
    mock_str_mvp_compet = read_txt("output_message_calculated_mvp_race_figures.txt")
    mock_str_mvp_compet_gameday = read_txt("output_message_calculated_list_gameday_with_predict.txt")
    
    with patch.object(output_message_calculated_generation,"get_games_result", return_value=(mock_str_games_result, 2)), \
         patch.object(output_message_calculated_generation,"get_scores_detailed", return_value=(mock_df_predict_games, 2)), \
         patch.object(output_message_calculated_generation,"snowflake_execute", side_effect=[mock_df_userscores_global, mock_df_list_gameday, mock_df_gamepredictchamp]), \
         patch.object(output_message_calculated_generation,"get_scores_global", return_value=(mock_df_scores_global, 2)), \
         patch.object(output_message_calculated_generation,"get_scores_average", return_value=(mock_str_scores_average, 1, 33)), \
         patch.object(output_message_calculated_generation,"get_scores_gameday", return_value=(mock_df_scores_gameday, 2)), \
         patch.object(output_message_calculated_generation,"get_list_gameday", return_value=mock_str_list_gameday), \
         patch.object(output_message_calculated_generation,"get_predictchamp_result", return_value=mock_str_predictchamp_result), \
         patch.object(output_message_calculated_generation,"get_predictchamp_ranking", return_value=mock_df_predictchamp_rank), \
         patch.object(output_message_calculated_generation,"get_correction", return_value=(mock_str_correction, 2)), \
         patch.object(output_message_calculated_generation,"get_mvp_month_race_figure", return_value=("__L__MONTH_01__L__", mock_str_mvp_month, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_month_race_gameday", return_value=(mock_str_mvp_month_gameday)), \
         patch.object(output_message_calculated_generation,"get_mvp_compet_race_figure", return_value=(mock_str_mvp_compet, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_compet_race_gameday", return_value=(mock_str_mvp_compet_gameday)):

        output_message_calculated_generation.get_parameters(sr_snowflake_account_connect, sr_gameday_output_calculate)

def test_get_parameters_df_management(read_csv, read_json):
    
    # this test the function get_parameters_df_management
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    country = "FRANCE"
    forum = "BI"
    translations = read_json("output_gameday_template_translations.json")

    param_dict = {
        "SCORES_DETAILED_DF": read_csv("output_message_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False),
        "SCORES_GLOBAL_DF": read_csv("output_message_calculated_scores_global.csv"),
        "SCORES_GAMEDAY_DF": read_csv("output_message_calculated_scores_gameday.csv"),
        "RANK_PREDICTCHAMP_DF": read_csv("output_message_calculated_predictchamp_rank.csv"),
        "IS_FOR_RANK": 1,
    }
    with patch.object(output_message_calculated_generation.output,"manage_df", side_effect=["url1", "url2", "url3","url4"]) as mock_manage_df:
         
        result = output_message_calculated_generation.get_parameters_df_management(param_dict,sr_gameday_output_calculate,country,forum, translations)

        assert result["SCORES_DETAILED_DF_URL_FRANCE_BI"] == "url1"
        assert result["SCORES_GLOBAL_DF_URL_FRANCE_BI"] == "url2"
        assert result["SCORES_GAMEDAY_DF_URL_FRANCE_BI"] == "url3"
        assert result["RANK_PREDICTCHAMP_DF_URL_FRANCE_BI"] == "url4"
        assert mock_manage_df.call_count == 4

def test_create_message(read_txt, read_json, read_csv):
    
    # this test the function create_messages_for_country with all parameters
    param_dict = {
        "GAMEDAY": "1ere journee",
        "SEASON_DIVISION": "PROB",
        "RESULT_GAMES": read_txt("output_message_calculated_games_result.txt"),
        "NB_GAMEDAY_CALCULATED": 3,
        "NB_MAX_PREDICT": 8,
        "NB_TOTAL_PREDICT": 66,
        "LIST_GAMEDAY_CALCULATED": read_txt("output_message_calculated_list_gameday.txt"),
        "NB_USER_DETAIL": 2,
        "SCORES_GAMEDAY_DF_URL_FRANCE_BI": "url_gameday",
        "SCORES_DETAILED_DF_URL_FRANCE_BI": "url_detail",
        "NB_GAMES": 2,
        "NB_CORRECTION": 2,
        "LIST_CORRECTION": read_txt("output_message_calculated_correction.txt"),
        "NB_USER_GLOBAL": 2,
        "SCORES_GLOBAL_DF_URL_FRANCE_BI": "url_global",
        "NB_USER_AVERAGE": 1,
        "NB_MIN_PREDICTION": 33,
        "SCORES_AVERAGE": read_txt("output_message_calculated_scores_average.txt"),
        "NB_GAME_PREDICTCHAMP": 2,
        "RESULTS_PREDICTCHAMP": read_txt("output_message_calculated_predictchamp_result.txt"),   
        "IS_FOR_RANK": 1,
        "RANK_PREDICTCHAMP_DF_URL_FRANCE_BI": "url_predictchamp",
        "NB_USER_MONTH": 2,
        "GAMEDAY_MONTH": "__L__MONTH_01__L__",
        "LIST_USER_MONTH": read_txt("output_message_calculated_mvp_race_figures.txt"),   
        "LIST_GAMEDAY_MONTH": read_txt("output_message_calculated_list_gameday_with_predict.txt"),
        "NB_USER_COMPETITION": 2,
        "GAMEDAY_COMPETITION": "__L__REGULAR SEASON__L__",
        "IS_SAME_FOR_PREDICTCHAMP": 1,
        "LIST_USER_COMPETITION": read_txt("output_message_calculated_mvp_race_figures.txt"), 
        "LIST_GAMEDAY_COMPETITION": read_txt("output_message_calculated_list_gameday_with_predict.txt"),
        "HAS_HOME_ADV": 1
    }
    template = read_txt("output_gameday_calculation_template_france.txt")
    translations = read_json("output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_filename = "result.txt"
    expected_result = read_txt("forumoutput_calculated_s1_1erejournee_france_bi.txt")

    with patch.object(output_message_calculated_generation.output,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_calculated_generation.files_manipulation,"create_txt"):

          
        content, country, forum = output_message_calculated_generation.create_message(param_dict, template, translations, country, forum, sr_gameday_output_calculate)
        assert content.split() == expected_result.split()
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_calculated(read_yml_as_serie, read_txt, read_json, read_csv):
    
    # this test the function process_output_message_calculated
    context_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'str_output_gameday_calculation_template_france': read_txt("output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_italia': read_txt("output_gameday_calculation_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("output_gameday_template_translations.json")
    }
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = read_csv("q_vw_topic_calculate_query.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_params_df_retrieved = [{
        'df1_FRANCE_BI':"url1",
        'df2_FRANCE_BI':"url2"
    },{
        'df1_ITALIA_II':"url3",
        'df2_ITALIA_II':"url4"        
    }]
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch.object(output_message_calculated_generation,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_calculated_generation,"get_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_calculated_generation,"multithread_run", side_effect=[mock_params_df_retrieved, mock_messages,None]):

        output_message_calculated_generation.process_output_message_calculated(context_dict, sr_gameday_output_calculate)
