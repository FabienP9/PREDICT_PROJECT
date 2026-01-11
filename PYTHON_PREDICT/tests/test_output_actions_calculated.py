'''
This tests file concern all functions in the output_actions_calculated module.
It units test the happy path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os
from pandas.testing import assert_frame_equal

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_calculated
from testutils import read_txt
from testutils import read_json

def test_get_calculated_games_result():

    # this test the function get_calculated_games_result   
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_qgame = pd.read_csv("materials/qGame.csv")

    with patch.object(output_actions_calculated, 'snowflake_execute', return_value=mock_df_qgame):
        s, count = output_actions_calculated.get_calculated_games_result(sr_snowflake_account, sr_gameday_output_calculate)
        s_expected = read_txt("materials/output_calculated_get_calculated_games_result.txt")
        
        assert count == 2
        assert s == s_expected

def test_capture_scores_detailed():

    # this test the function capture_scores_detailed
    df = pd.read_csv("materials/table_scores_details.csv", header=[0, 1])
    capture_name = "mycapture"

    with patch.object(output_actions_calculated.fileA, 'create_jpg'):
        output_actions_calculated.capture_scores_detailed(df, capture_name)

def test_get_calculated_scores_detailed():

    # this test the function get_calculated_scores_detailed
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_predict_games = pd.read_csv("materials/qPredictGame.csv",keep_default_na=False,na_filter=False)
    df_predict_games_expected = pd.read_csv("materials/table_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_predict_games):
        df_result, n_users = output_actions_calculated.get_calculated_scores_detailed(sr_snowflake_account, sr_gameday_output_calculate)
        
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

def test_get_calculated_scores_global():

    # this test the function get_calculated_scores_global
    df_userscores_global = pd.read_csv("materials/qUserScores_Global.csv")
    mock_df_rank = pd.read_csv("materials/qUserScoresGlobal_ranked.csv")
    df_expected = pd.read_csv("materials/output_calculated_get_calculated_scores_global.csv")
    
    with patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_rank):
        df_result,nb_result = output_actions_calculated.get_calculated_scores_global(df_userscores_global)

    assert_frame_equal(df_result.reset_index(drop=True), df_expected.reset_index(drop=True))
    assert nb_result == 2

def test_get_calculated_scores_gameday():
    
    # this test the function get_calculated_scores_gameday
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = pd.read_csv("materials/qUserScoresGameday.csv")
    mock_df_userscores_gameday_ranked = pd.read_csv("materials/qUserScoresGameday.csv")
    expected_str = read_txt("materials/output_calculated_get_calculated_scores_gameday.txt")
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            result_str, n_users = output_actions_calculated.get_calculated_scores_gameday(sr_snowflake_account, sr_gameday_output_calculate)
            assert n_users == 2
            assert result_str == expected_str

def test_get_calculated_scores_average():

    # this test the function get_calculated_scores_average
    nb_prediction = 67
    df_userscores_global = pd.read_csv("materials/qUserScores_Global.csv")
    mock_df_rank = pd.read_csv("materials/qUserScoresAverage_ranked.csv")
    expected_str = read_txt("materials/output_actions_get_calculated_scores_average.txt")
    
    with patch.object(output_actions_calculated.outputA, 'calculate_and_display_rank', return_value= mock_df_rank):
        s, count, nb_min = output_actions_calculated.get_calculated_scores_average(nb_prediction, df_userscores_global)
        assert s == expected_str
        assert nb_min == 33
        assert count == 1

def test_get_calculated_predictchamp_result():
    
    # this test the function get_calculated_predictchamp_result
    df_gamepredictchamp = pd.read_csv("materials/qGamePredictchamp.csv")
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_detail = pd.read_csv("materials/qGamePredictchampDetail.csv")
    expected_str = read_txt("materials/output_actions_calculated_get_calculated_predictchamp_result.txt")

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_detail):
        result = output_actions_calculated.get_calculated_predictchamp_result(df_gamepredictchamp, sr_snowflake_account, sr_gameday_output_calculate)
        assert result == expected_str

def test_get_calculated_predictchamp_ranking():
    
    # this test the function get_calculated_predictchamp_ranking
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_teamscores = pd.read_csv("materials/qTeamScores.csv")
    mock_df_rank = pd.read_csv("materials/qTeamScores_ranked.csv")

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_teamscores):
        with patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_rank) as mock_rank:
            result_df = output_actions_calculated.get_calculated_predictchamp_ranking(sr_snowflake_account, sr_gameday_output_calculate)

            mock_rank.assert_called_once_with(mock_df_teamscores, 'RANK')
            pd.testing.assert_frame_equal(result_df, mock_df_rank)

def test_get_calculated_correction():
    
    # this test the function get_calculated_correction
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_correction = pd.read_csv("materials/qCorrection.csv")
    expected_str = read_txt("materials/output_actions_calculated_get_calculated_correction.txt")

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_correction):
        result_str, result_count = output_actions_calculated.get_calculated_correction(sr_snowflake_account, sr_gameday_output_calculate)

        assert result_str == expected_str
        assert result_count == 2

def test_get_calculated_list_gameday():
    
    # this test the function get_calculated_list_gameday
    df_list_gameday = pd.read_csv("materials/qList_Gameday_Calculated.csv")
    expected_str = read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt")
    str_list_gameday = output_actions_calculated.get_calculated_list_gameday(df_list_gameday)
    assert str_list_gameday == expected_str

def test_get_mvp_month_race_figure():
    
    # this test the function get_calculated_predictchamp_ranking
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = pd.read_csv("materials/qMVPRace_figures.csv",quotechar='"',keep_default_na=False)
    expected_str = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")

    # mock snowflake_execute to return df_teamscores
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        assert gameday_month == "MONTH_01"
        assert count == 3
        assert list_user == expected_str

def test_get_mvp_compet_race_figure():
    
    # this test the function get_calculated_predictchamp_ranking
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_compet_mvp = pd.read_csv("materials/qMVPRace_figures.csv",quotechar='"',keep_default_na=False)
    expected_str = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")

    # mock snowflake_execute to return df_teamscores
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_compet_mvp):
        (compet, list_user, count) = output_actions_calculated.get_mvp_compet_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        
        assert compet == "Regular season"
        assert count == 3
        assert list_user == expected_str

def test_get_calculated_parameters():

    # this test the function get_calculated_parameters mocking all functions
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_str_games_result = read_txt("materials/output_calculated_get_calculated_games_result.txt")
    mock_df_predict_games = pd.read_csv("materials/table_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_df_userscores_global = pd.read_csv("materials/qUserScores_Global.csv")
    mock_df_scores_global = pd.read_csv("materials/output_calculated_get_calculated_scores_global.csv")
    mock_df_list_gameday = pd.read_csv("materials/qList_Gameday_Calculated.csv")
    mock_str_scores_average = read_txt("materials/output_actions_get_calculated_scores_average.txt")
    mock_str_scores_gameday = read_txt("materials/output_calculated_get_calculated_scores_gameday.txt")
    mock_str_list_gameday = read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt")
    mock_df_gamepredictchamp = pd.read_csv("materials/qGamePredictchamp.csv")
    mock_str_predictchamp_result = read_txt("materials/output_actions_calculated_get_calculated_predictchamp_result.txt")
    mock_df_predictchamp_rank = pd.read_csv("materials/qTeamScores_ranked.csv")
    mock_str_correction = read_txt("materials/output_actions_calculated_get_calculated_correction.txt")
    mock_str_mvp_month = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")
    mock_str_mvp_compet = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")

    with patch("output_actions_calculated.get_calculated_games_result", return_value=(mock_str_games_result, 2)), \
         patch("output_actions_calculated.get_calculated_scores_detailed", return_value=(mock_df_predict_games, 2)), \
         patch("output_actions_calculated.snowflake_execute", side_effect=[mock_df_userscores_global, mock_df_list_gameday, mock_df_gamepredictchamp]), \
         patch("output_actions_calculated.get_calculated_scores_global", return_value=(mock_df_scores_global, 2)), \
         patch("output_actions_calculated.get_calculated_scores_average", return_value=(mock_str_scores_average, 1, 33)), \
         patch("output_actions_calculated.get_calculated_scores_gameday", return_value=(mock_str_scores_gameday, 2)), \
         patch("output_actions_calculated.get_calculated_list_gameday", return_value=mock_str_list_gameday), \
         patch("output_actions_calculated.get_calculated_predictchamp_result", return_value=mock_str_predictchamp_result), \
         patch("output_actions_calculated.get_calculated_predictchamp_ranking", return_value=mock_df_predictchamp_rank), \
         patch("output_actions_calculated.get_calculated_correction", return_value=(mock_str_correction, 2)), \
         patch("output_actions_calculated.get_mvp_month_race_figure", return_value=("MONTH_01", mock_str_mvp_month, 2)), \
         patch("output_actions_calculated.get_mvp_compet_race_figure", return_value=("Regular season", mock_str_mvp_compet, 2)):

        output_actions_calculated.get_calculated_parameters(sr_snowflake_account, sr_gameday_output_calculate)

def test_derive_calculated_parameters_for_country():
    
    # this test the function derive_calculated_parameters_for_country
    SCORES_DETAILED_DF = pd.read_csv("materials/table_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    SCORES_GLOBAL_DF = pd.read_csv("materials/output_calculated_get_calculated_scores_global.csv")
    RANK_PREDICTCHAMP_DF = pd.read_csv("materials/qTeamScores_ranked.csv")
    param_dict = {
        "SCORES_DETAILED_DF": SCORES_DETAILED_DF,
        "SCORES_GLOBAL_DF": SCORES_GLOBAL_DF,
        "RANK_PREDICTCHAMP_DF": RANK_PREDICTCHAMP_DF,
        "OTHER_PARAM": "value"
    }
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    country = "FRANCE"
    translations = read_json("../output_actions_translations.json")

    mock_SCORES_DETAILED_DF_FRANCE = pd.read_csv("materials/table_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_SCORES_GLOBAL_DF_FRANCE = pd.read_csv("materials/output_calculated_get_calculated_scores_global_france.csv")
    mock_RANK_PREDICTCHAMP_DF_FRANCE = pd.read_csv("materials/qTeamScores_ranked_FRANCE.csv")
    mock_OTHER_PARAM_FRANCE = "value"

    mock_name_SCORE_DETAILED_DF_FRANCE = "table_scores_detailed_1erejournee_france.jpg"
    mock_name_SCORES_GLOBAL_DF_FRANCE = "table_scores_global_1erejournee_france.jpg"
    mock_name_RANK_PREDICTCHAMP_DF_FRANCE = "table_scores_detailed_1erejournee_france.jpg"
    
    mock_url_SCORE_DETAILED_DF_FRANCE = "url1"
    mock_url_SCORES_GLOBAL_DF_FRANCE = "url2"
    mock_url_RANK_PREDICTCHAMP_DF_FRANCE = "url3"

    with patch.object(output_actions_calculated.outputA, "translate_param_for_country", side_effect=[mock_SCORES_DETAILED_DF_FRANCE, mock_SCORES_GLOBAL_DF_FRANCE, mock_RANK_PREDICTCHAMP_DF_FRANCE, mock_OTHER_PARAM_FRANCE]), \
         patch("output_actions_calculated.capture_scores_detailed"), \
         patch.object(output_actions_calculated.outputA, "capture_df_oneheader"), \
         patch.object(output_actions_calculated.outputA, "define_filename", side_effect=[mock_name_SCORE_DETAILED_DF_FRANCE, mock_name_SCORES_GLOBAL_DF_FRANCE, mock_name_RANK_PREDICTCHAMP_DF_FRANCE]), \
         patch.object(output_actions_calculated, "push_capture_online", side_effect=[mock_url_SCORE_DETAILED_DF_FRANCE, mock_url_SCORES_GLOBAL_DF_FRANCE, mock_url_RANK_PREDICTCHAMP_DF_FRANCE]):

        output_actions_calculated.derive_calculated_parameters_for_country(
            param_dict, sr_gameday_output_calculate, country, translations
        )

def test_derive_calculated_parameters():
    
    # this test the function derive_calculated_parameters
    param_dict = {
        'SCORES_DETAILED_DF': pd.DataFrame([[1, 2]]),
        'SCORES_GLOBAL_DF': pd.DataFrame([[3, 4]]),
        'RANK_PREDICTCHAMP_DF': pd.DataFrame([[5, 6]]),
        'RESULTS_PREDICTCHAMP': "some_result",
        'OTHER_PARAM': "should_not_be_derived"
    }
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    list_countries = ['FRANCE']
    mock_translations = read_json("../output_actions_translations.json")

    mock_SCORES_DETAILED_DF_FRANCE = pd.read_csv("materials/table_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_SCORES_GLOBAL_DF_FRANCE = pd.read_csv("materials/output_calculated_get_calculated_scores_global_france.csv")
    mock_RANK_PREDICTCHAMP_DF_FRANCE = pd.read_csv("materials/qTeamScores_ranked_FRANCE.csv")
    mock_OTHER_PARAM_FRANCE = "value"
    mock_url_SCORE_DETAILED_DF_FRANCE = "url1"
    mock_url_SCORES_GLOBAL_DF_FRANCE = "url2"
    mock_url_RANK_PREDICTCHAMP_DF_FRANCE = "url3"
    mock_result_dicts = [
        {"SCORES_DETAILED_DF": mock_SCORES_DETAILED_DF_FRANCE},
        {"SCORES_GLOBAL_DF": mock_SCORES_GLOBAL_DF_FRANCE},
        {"RANK_PREDICTCHAMP_DF": mock_RANK_PREDICTCHAMP_DF_FRANCE},
        {"OTHER_PARAM": mock_OTHER_PARAM_FRANCE},
        {"URL_SCORE_DETAILED_DF": mock_url_SCORE_DETAILED_DF_FRANCE},
        {"URL_SCORES_GLOBAL_DF": mock_url_SCORES_GLOBAL_DF_FRANCE},
        {"URL_RANK_PREDICTCHAMP_DF": mock_url_RANK_PREDICTCHAMP_DF_FRANCE},
    ]

    with patch("output_actions_calculated.fileA.read_json", return_value=mock_translations), \
         patch("output_actions_calculated.config.multithreading_run", return_value=mock_result_dicts):

        output_actions_calculated.derive_calculated_parameters(param_dict, sr_gameday_output_calculate, list_countries)

def test_create_calculated_messages_for_country():
    
    # this test the function create_calculated_messages_for_country with all parameters
    param_dict = {
        "GAMEDAY": "1ere journee",
        "SEASON_DIVISION": "PROB",
        "RESULT_GAMES": read_txt("materials/output_calculated_get_calculated_games_result.txt"),
        "NB_GAMEDAY_CALCULATED": 3,
        "NB_TOTAL_PREDICT": 66,
        "NB_MAX_PREDICT": 8,
        "LIST_GAMEDAY_CALCULATED": read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt"),
        "NB_USER_DETAIL": 2,
        "SCORES_GAMEDAY": read_txt("materials/output_calculated_get_calculated_scores_gameday.txt"),
        "SCORES_DETAILED_DF_URL_FRANCE": "url_detail",
        "NB_GAMES": 2,
        "NB_CORRECTION": 2,
        "LIST_CORRECTION": read_txt("materials/output_actions_calculated_get_calculated_correction.txt"),
        "NB_USER_GLOBAL": 2,
        "SCORES_GLOBAL_DF_URL_FRANCE": "url_global",
        "NB_USER_AVERAGE": 1,
        "NB_MIN_PREDICTION": 33,
        "SCORES_AVERAGE": read_txt("materials/output_actions_get_calculated_scores_average.txt"),
        "NB_GAME_PREDICTCHAMP": 2,
        "RESULTS_PREDICTCHAMP_FRANCE": read_txt("materials/output_actions_calculated_get_calculated_predictchamp_result.txt"),   
        "IS_FOR_RANK": 1,
        "RANK_PREDICTCHAMP_DF_URL_FRANCE": "url_predictchamp",
        "NB_USER_MONTH": 2,
        "GAMEDAY_MONTH_FRANCE": "Janvier",
        "LIST_USER_MONTH_FRANCE": read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt"),   
        "NB_USER_COMPETITION": 2,
        "GAMEDAY_COMPETITION_FRANCE": "Regular season",
        "LIST_USER_COMPETITION_FRANCE": read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt"), 
        "HAS_HOME_ADV": 1
    }
    country = "FRANCE"
    template = read_txt("materials/output_gameday_calculation_template_france.txt")
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_str_format_message = template
    mock_filename = "result.txt"
    expected_result = read_txt("materials/forumoutput_calculated_s1_1erejournee_france.txt")

    with patch("output_actions_calculated.outputA.format_message", return_value=mock_str_format_message), \
         patch("output_actions_calculated.outputA.replace_conditionally_message", side_effect=lambda c, b, e, cond: c) as mock_replace, \
         patch("output_actions_calculated.outputA.define_filename", return_value=mock_filename) as mock_filename, \
         patch("output_actions_calculated.fileA.create_txt") as mock_create_txt:
        
        content, result_country = output_actions_calculated.create_calculated_messages_for_country(
            param_dict, country, template, sr_gameday_output_calculate
        )
        assert expected_result == content
        assert result_country == "FRANCE"

def test_process_output_message_calculated():
    
    # this test the function process_output_message_calculated
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_calculation_template_FRANCE': read_txt("materials/output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_ITALIA': read_txt("materials/output_gameday_calculation_template_france.txt")
    }
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/qTopics_Calculate.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_params_derived = {'dummy':'param_derived'}
    mock_messages = [("fake_content_FRANCE", "FRANCE"), ("fake_content_ITALIA", "ITALIA")]

    # Patch all external dependencies
    with patch("output_actions_calculated.snowflake_execute", return_value=mock_df_topics), \
         patch("output_actions_calculated.get_calculated_parameters", return_value=mock_params_retrieved), \
         patch("output_actions_calculated.derive_calculated_parameters", return_value=mock_params_derived), \
         patch("output_actions_calculated.config.multithreading_run", return_value=mock_messages), \
         patch("output_actions_calculated.post_message"):

        # Call the function
        output_actions_calculated.process_output_message_calculated(context_dict, sr_gameday_output_calculate)

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result))
    test_suite.addTest(unittest.FunctionTestCase(test_capture_scores_detailed))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_detailed))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_result))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_ranking))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_correction))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_compet_race_figure))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_derive_calculated_parameters))
    test_suite.addTest(unittest.FunctionTestCase(test_create_calculated_messages_for_country))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)