'''
This tests file concern all functions in the output_actions_calculated module.
It units test unexpected path for each function
'''
import unittest
from unittest.mock import patch
import pandas as pd
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import output_actions_calculated
from testutils import assertExit
from testutils import read_txt
from testutils import read_json

def test_get_calculated_games_result_empty_df():
    
    # this test the function get_calculated_games_result with an empty series. Must return an empty string.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_qgame = pd.read_csv("materials/edgecases/qGame_empty.csv")
    
    with patch.object(output_actions_calculated, 'snowflake_execute', return_value=mock_df_qgame):
        s, count = output_actions_calculated.get_calculated_games_result(sr_snowflake_account, sr_gameday_output_calculate)
        assert count == 0
        assert s == ""

def test_get_calculated_games_result_negative_result():

    # this test the function get_calculated_games_result with a negative result.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    s_expected = read_txt("materials/edgecases/output_calculated_get_calculated_games_result_negative.txt")
    mock_df_qgame = pd.read_csv("materials/edgecases/qGame_negative.csv")

    with patch.object(output_actions_calculated, 'snowflake_execute', return_value=mock_df_qgame):
        s, count = output_actions_calculated.get_calculated_games_result(sr_snowflake_account, sr_gameday_output_calculate)
        assert count == 1
        assert s == s_expected
    
def test_get_calculated_scores_detailed_missing_split():
    
    # this test the function get_calculated_scores_detailed with columns without underscore. Must exit the program.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_predict_games = pd.read_csv("materials/edgecases/qPredictGame_columns_nounderscores.csv",keep_default_na=False,na_filter=False)
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_predict_games):
        assertExit(lambda: output_actions_calculated.get_calculated_scores_detailed(sr_snowflake_account, sr_gameday_output_calculate))

def test_get_calculated_scores_global_empty_df():
    
    # this test the function get_calculated_scores_global with empty dataframe. Must exit the program.
    df_userscores_global = pd.read_csv("materials/edgecases/qUserScores_Global_empty.csv")
    mock_df_rank = pd.read_csv("materials/edgecases/qUserScoresGlobal_ranked_empty.csv")
    
    with patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_rank):
        df_result,nb_result = output_actions_calculated.get_calculated_scores_global(df_userscores_global)

        assert df_result.empty
        assert nb_result == 0
    
def test_get_calculated_scores_global_missing_cols():
    
    # this test the function get_calculated_scores_global with missing columns. Must exit the program.
    df_userscores_global = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_rank = pd.DataFrame({"USER_NAME":["X"]})

    with patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_rank):
        assertExit(lambda: output_actions_calculated.get_calculated_scores_global(df_userscores_global))

def test_get_calculated_scores_gameday_empty():
    
    # this test the function get_calculated_scores_gameday with empty dataframe. Must return an empty string.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = pd.read_csv("materials/edgecases/qUserScoresGameday_empty.csv")
    mock_df_userscores_gameday_ranked = pd.read_csv("materials/edgecases/qUserScoresGameday_empty.csv")
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            df_result, nb_result = output_actions_calculated.get_calculated_scores_gameday(sr_snowflake_account, sr_gameday_output_calculate)
            assert df_result.empty
            assert nb_result == 0

def test_get_calculated_scores_gameday_missing_column():
    
    # this test the function get_calculated_scores_gameday with dataframe having missing columns. Must exit the program.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_userscores_gameday_ranked = pd.DataFrame({"USER_NAME":["X"]})
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            assertExit(lambda: output_actions_calculated.get_calculated_scores_gameday(sr_snowflake_account, sr_gameday_output_calculate))

def test_get_calculated_scores_average_empty_df():
    
    # this test the function get_calculated_scores_average with empty dataframe. Must return an empty string
    nb_prediction = 4
    df_userscores_global = pd.read_csv("materials/edgecases/qUserScores_Global_empty.csv")
    mock_df_rank = pd.read_csv("materials/edgecases/qUserScoresAverage_ranked_empty.csv")
    
    with patch.object(output_actions_calculated.outputA, 'calculate_and_display_rank', return_value= mock_df_rank):
        s, count, nb_min = output_actions_calculated.get_calculated_scores_average(nb_prediction, df_userscores_global)
        assert s == ""
        assert nb_min == 2
        assert count == 0

def test_get_calculated_scores_average_missing_column():
    
    # this test the function get_calculated_scores_average with a dataframe with missing columns. Must exit the program
    nb_prediction = 4
    df_userscores_global = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_rank = pd.DataFrame({"USER_NAME":["X"]})
    
    with patch.object(output_actions_calculated.outputA, 'calculate_and_display_rank', return_value= mock_df_rank):
        assertExit(lambda: output_actions_calculated.get_calculated_scores_average(nb_prediction, df_userscores_global))

def test_get_calculated_predictchamp_ranking_empty():
    
    # this test the function get_calculated_predictchamp_ranking an empty dataframe. Must return an empty dataframe
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_teamscores = pd.read_csv("materials/edgecases/qTeamScores_empty.csv")
    mock_df_rank = pd.read_csv("materials/edgecases/qTeamScores_ranked_empty.csv")

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_teamscores):
        with patch.object(output_actions_calculated.outputA, "display_rank", return_value=mock_df_rank) as mock_rank:
            
            result_df = output_actions_calculated.get_calculated_predictchamp_ranking(sr_snowflake_account, sr_gameday_output_calculate)
            assert result_df.empty

def test_get_calculated_correction_no_rows():
    
    # this test the function get_calculated_correction with no rows. Must return an empty string
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_correction = pd.read_csv("materials/edgecases/qCorrection_empty.csv")
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_correction):
        result_str, result_count = output_actions_calculated.get_calculated_correction(sr_snowflake_account, sr_gameday_output_calculate)

        assert result_str == ""
        assert result_count == 0
 
def test_get_calculated_list_gameday_missing_columns():
    
    # this test the function get_calculated_list_gameday with missing columns. Must exit the program
    df_list_gameday = pd.DataFrame({ "NB_PREDICTION": [3]})
    assertExit(lambda: output_actions_calculated.get_calculated_list_gameday(df_list_gameday))

def test_get_calculated_list_gameday_empty_dataframe():
    
    # this test the function get_calculated_list_gameday with empty df. Must return an empty string
    df_list_gameday = pd.read_csv("materials/edgecases/qList_Gameday_Calculated_empty.csv")
    result = output_actions_calculated.get_calculated_list_gameday(df_list_gameday)
    assert result == ""

def test_get_mvp_month_race_figure_empty_df():
    
    # this function test the function get_mvp_month_race_figure with an empty return from query. Must return an empty string
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = pd.read_csv("materials/edgecases/qMVPRace_figures_empty.csv",quotechar='"')

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        assert gameday_month == "__L__MONTH_01__L__"
        assert count == 0
        assert list_user == ""

def test_get_mvp_month_race_figure_invalid_points():
    
    # this function test the function get_mvp_month_race_figure with invalid points. Must accept, but should not happen due to test in database which stop the program if not an int.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = pd.read_csv("materials/edgecases/qMVPRace_figures_invalid_points.csv",quotechar='"')
    expected_str = read_txt("materials/edgecases/output_actions_calculated_get_mvp_race_figures_invalid_points.txt")

    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_actions_calculated.get_mvp_month_race_figure(sr_snowflake_account, sr_gameday_output_calculate)
        assert gameday_month == "__L__MONTH_01__L__"
        assert count == 2
        assert list_user == expected_str

def test_get_mvp_compet_race_figure_missing_key():

    # this function test the function get_mvp_month_race_figure with missing column in sr_gameday_output_calculate. Must exit the program
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/edgecases/sr_gameday_output_calculate_nocompetitionlabel.csv").iloc[0]
    mock_df_compet_mvp = pd.read_csv("materials/qMVPRace_figures.csv",quotechar='"')
    
    with patch.object(output_actions_calculated, "snowflake_execute", return_value=mock_df_compet_mvp):
        assertExit(lambda: output_actions_calculated.get_mvp_compet_race_figure(sr_snowflake_account, sr_gameday_output_calculate))

def test_get_calculated_parameters_no_predictchamp():

    # this test get_calculated_parameters without predictchamp results and rank (empty df_gamepredictchamp). Must not call it
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]

    mock_str_games_result = read_txt("materials/output_actions_calculated_get_calculated_games_result.txt")
    mock_df_predict_games = pd.read_csv("materials/output_actions_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_df_userscores_global = pd.read_csv("materials/qUserScores_Global.csv")
    mock_df_scores_global = pd.read_csv("materials/output_actions_calculated_get_calculated_scores_global.csv")
    mock_df_list_gameday = pd.read_csv("materials/qList_Gameday_Calculated.csv")
    mock_str_scores_average = read_txt("materials/output_actions_get_calculated_scores_average.txt")
    mock_df_scores_gameday = pd.read_csv("materials/output_actions_calculated_scores_gameday.csv")
    mock_str_list_gameday = read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt")
    mock_df_gamepredictchamp = pd.read_csv("materials/edgecases/qGamePredictchamp_empty.csv")
    mock_str_correction = read_txt("materials/output_actions_calculated_get_calculated_correction.txt")
    mock_str_mvp_month = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")
    mock_str_mvp_compet = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")

    with patch("output_actions_calculated.get_calculated_games_result", return_value=(mock_str_games_result, 2)), \
         patch("output_actions_calculated.get_calculated_scores_detailed", return_value=(mock_df_predict_games, 2)), \
         patch("output_actions_calculated.snowflake_execute", side_effect=[mock_df_userscores_global, mock_df_list_gameday, mock_df_gamepredictchamp]), \
         patch("output_actions_calculated.get_calculated_scores_global", return_value=(mock_df_scores_global, 2)), \
         patch("output_actions_calculated.get_calculated_scores_average", return_value=(mock_str_scores_average, 1, 33)), \
         patch("output_actions_calculated.get_calculated_scores_gameday", return_value=(mock_df_scores_gameday, 2)), \
         patch("output_actions_calculated.get_calculated_list_gameday", return_value=mock_str_list_gameday), \
         patch("output_actions_calculated.get_calculated_predictchamp_result") as mock_predictchamp_result, \
         patch("output_actions_calculated.get_calculated_predictchamp_ranking") as mock_predictchamp_ranking, \
         patch("output_actions_calculated.get_calculated_correction", return_value=(mock_str_correction, 2)), \
         patch("output_actions_calculated.get_mvp_month_race_figure", return_value=("__L__MONTH_01__L__", mock_str_mvp_month, 2)), \
         patch("output_actions_calculated.get_mvp_compet_race_figure", return_value=("__L__REGULAR SEASON__L__", mock_str_mvp_compet, 2)):
        
        params = output_actions_calculated.get_calculated_parameters(sr_snowflake_account, sr_gameday_output_calculate)
        mock_predictchamp_result.assert_not_called()
        mock_predictchamp_ranking.assert_not_called()

def test_get_calculated_parameters_missing_key():
    
    # this test the function get_calculated_parameters with missing key in sr_gameday_output_calculate. Must exit the program.
    sr_snowflake_account = pd.read_csv("materials/snowflake_account_connect.csv").iloc[0]
    sr_gameday_output_calculate = pd.read_csv("materials/edgecases/sr_gameday_output_calculate_noseasonid.csv").iloc[0]

    mock_str_games_result = read_txt("materials/output_actions_calculated_get_calculated_games_result.txt")
    mock_df_predict_games = pd.read_csv("materials/output_actions_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_df_userscores_global = pd.read_csv("materials/qUserScores_Global.csv")
    mock_df_scores_global = pd.read_csv("materials/output_actions_calculated_get_calculated_scores_global.csv")
    mock_df_list_gameday = pd.read_csv("materials/qList_Gameday_Calculated.csv")
    mock_str_scores_average = read_txt("materials/output_actions_get_calculated_scores_average.txt")
    mock_df_scores_gameday = pd.read_csv("materials/output_actions_calculated_scores_gameday.csv")
    mock_str_list_gameday = read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt")
    mock_df_gamepredictchamp = pd.read_csv("materials/qGamePredictchamp.csv")
    mock_str_predictchamp_result = read_txt("materials/output_actions_calculated_get_calculated_predictchamp_result.txt")
    mock_df_predictchamp_rank = pd.read_csv("materials/output_actions_calculated_predictchamp_rank.csv")
    mock_str_correction = read_txt("materials/output_actions_calculated_get_calculated_correction.txt")
    mock_str_mvp_month = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")
    mock_str_mvp_compet = read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt")

    with patch("output_actions_calculated.get_calculated_games_result", return_value=(mock_str_games_result, 2)), \
         patch("output_actions_calculated.get_calculated_scores_detailed", return_value=(mock_df_predict_games, 2)), \
         patch("output_actions_calculated.snowflake_execute", side_effect=[mock_df_userscores_global, mock_df_list_gameday, mock_df_gamepredictchamp]), \
         patch("output_actions_calculated.get_calculated_scores_global", return_value=(mock_df_scores_global, 2)), \
         patch("output_actions_calculated.get_calculated_scores_average", return_value=(mock_str_scores_average, 1, 33)), \
         patch("output_actions_calculated.get_calculated_scores_gameday", return_value=(mock_df_scores_gameday, 2)), \
         patch("output_actions_calculated.get_calculated_list_gameday", return_value=mock_str_list_gameday), \
         patch("output_actions_calculated.get_calculated_predictchamp_result", return_value=mock_str_predictchamp_result), \
         patch("output_actions_calculated.get_calculated_predictchamp_ranking", return_value=mock_df_predictchamp_rank), \
         patch("output_actions_calculated.get_calculated_correction", return_value=(mock_str_correction, 2)), \
         patch("output_actions_calculated.get_mvp_month_race_figure", return_value=("MONTH_01", mock_str_mvp_month, 2)), \
         patch("output_actions_calculated.get_mvp_compet_race_figure", return_value=("Regular season", mock_str_mvp_compet, 2)):

        assertExit(lambda: output_actions_calculated.get_calculated_parameters(sr_snowflake_account, sr_gameday_output_calculate))

def test_create_calculated_message_conditional_blocks():
    
    # this test the function create_calculated_messages_for_country with a template without parameters. Must return the same result than the template
    param_dict = {
        "GAMEDAY": "1ere journee",
        "SEASON_DIVISION": "PROB",
        "RESULT_GAMES": read_txt("materials/output_actions_calculated_get_calculated_games_result.txt"),
        "NB_GAMEDAY_CALCULATED": 3,
        "NB_MAX_PREDICT": 8,
        "NB_TOTAL_PREDICT": 66,
        "LIST_GAMEDAY_CALCULATED": read_txt("materials/output_actions_calculated_get_list_gameday_calculated.txt"),
        "NB_USER_DETAIL": 2,
        "SCORES_GAMEDAY_DF_URL_FRANCE_BI": "url_gameday",
        "SCORES_DETAILED_DF_URL_FRANCE_BI": "url_detail",
        "NB_GAMES": 2,
        "NB_CORRECTION": 2,
        "LIST_CORRECTION": read_txt("materials/output_actions_calculated_get_calculated_correction.txt"),
        "NB_USER_GLOBAL": 2,
        "SCORES_GLOBAL_DF_URL_FRANCE_BI": "url_global",
        "NB_USER_AVERAGE": 1,
        "NB_MIN_PREDICTION": 33,
        "SCORES_AVERAGE": read_txt("materials/output_actions_get_calculated_scores_average.txt"),
        "NB_GAME_PREDICTCHAMP": 2,
        "RESULTS_PREDICTCHAMP": read_txt("materials/output_actions_calculated_get_calculated_predictchamp_result.txt"),   
        "IS_FOR_RANK": 1,
        "RANK_PREDICTCHAMP_DF_URL_FRANCE_BI": "url_predictchamp",
        "NB_USER_MONTH": 2,
        "GAMEDAY_MONTH": "__L__MONTH_01__L__",
        "LIST_USER_MONTH": read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt"),   
        "NB_USER_COMPETITION": 2,
        "GAMEDAY_COMPETITION": "__L__REGULAR SEASON__L__",
        "LIST_USER_COMPETITION": read_txt("materials/output_actions_calculated_get_mvp_race_figures.txt"), 
        "HAS_HOME_ADV": 1
    }
    template = "template"
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_filename = "result.txt"

    with patch("output_actions_calculated.outputA.define_filename", return_value=mock_filename) as mock_filename, \
         patch("output_actions_calculated.fileA.create_txt") as mock_create_txt:
        
        content, country, forum = output_actions_calculated.create_calculated_message(param_dict, template, country, forum, sr_gameday_output_calculate)
        
        assert content == template
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_calculated_no_topics():
    
    # this test the function process_output_message_calculated with no topics. Must run
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_calculation_template_FRANCE': read_txt("materials/output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_ITALIA': read_txt("materials/output_gameday_calculation_template_france.txt")
    }
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/edgecases/qTopics_Calculate_empty.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_params_df_retrieved = [{
        'df1_FRANCE_BI':"url1",
        'df2_FRANCE_BI':"url2"
    },{
        'df1_ITALIA_II':"url3",
        'df2_ITALIA_II':"url4"        
    }]
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch("output_actions_calculated.snowflake_execute", return_value=mock_df_topics), \
         patch("output_actions_calculated.get_calculated_parameters", return_value=mock_params_retrieved), \
         patch("output_actions_calculated.config.multithreading_run", side_effect=[mock_params_df_retrieved, mock_messages,None]):

        output_actions_calculated.process_output_message_calculated(context_dict, sr_gameday_output_calculate)

def test_process_output_message_calculated_posting_fails():
    
    # this test the function process_output_message_calculated with posting function failing. Must exit the program.
    context_dict = {
        'sr_snowflake_account_connect': pd.read_csv("materials/snowflake_account_connect.csv").iloc[0],
        'str_output_gameday_calculation_template_FRANCE': read_txt("materials/output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_ITALIA': read_txt("materials/output_gameday_calculation_template_france.txt")
    }
    sr_gameday_output_calculate = pd.read_csv("materials/sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = pd.read_csv("materials/qTopics_Calculate.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}
    mock_params_df_retrieved = [{
        'df1_FRANCE_BI':"url1",
        'df2_FRANCE_BI':"url2"
    },{
        'df1_ITALIA_II':"url3",
        'df2_ITALIA_II':"url4"        
    }]
    mock_messages = [("fake_content_FRANCE", "FRANCE", "BI"), ("fake_content_ITALIA", "ITALIA", "II")]

    with patch("output_actions_calculated.snowflake_execute", return_value=mock_df_topics), \
         patch("output_actions_calculated.get_calculated_parameters", return_value=mock_params_retrieved), \
         patch("output_actions_calculated.config.multithreading_run", side_effect=Exception("boom")):

        assertExit(lambda: output_actions_calculated.process_output_message_calculated(context_dict, sr_gameday_output_calculate))

if __name__ == '__main__':
    test_suite = unittest.TestSuite()
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_games_result_negative_result))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_detailed_missing_split))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_global_missing_cols))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_gameday_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_scores_average_missing_column))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_predictchamp_ranking_empty))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_correction_no_rows))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday_missing_columns))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_list_gameday_empty_dataframe))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure_empty_df))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_month_race_figure_invalid_points))
    test_suite.addTest(unittest.FunctionTestCase(test_get_mvp_compet_race_figure_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters_no_predictchamp))
    test_suite.addTest(unittest.FunctionTestCase(test_get_calculated_parameters_missing_key))
    test_suite.addTest(unittest.FunctionTestCase(test_create_calculated_message_conditional_blocks))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated_no_topics))
    test_suite.addTest(unittest.FunctionTestCase(test_process_output_message_calculated_posting_fails))
    runner = unittest.TextTestRunner()
    runner.run(test_suite)