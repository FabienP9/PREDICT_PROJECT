'''
This tests file concern all functions in the output_message_calculated_generation module.
It units test unexpected path
'''
import pandas as pd
from unittest.mock import patch

from src.predict_core.files_manipulation.local_files_manipulation.specific_files_operations.output_message_file_generation import output_message_calculated_generation

def test_get_games_result_empty_df(read_yml_as_serie, read_csv):
    
    # this test the function get_games_result with an empty series. Must return an empty string.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_game = read_csv("edgecases/q_vw_game_query_empty.csv")
    
    with patch.object(output_message_calculated_generation, 'snowflake_execute', return_value=mock_df_game):
        s, count = output_message_calculated_generation.get_games_result(sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert count == 0
        assert s == ""

def test_get_games_result_negative_result(read_yml_as_serie, read_csv,read_txt):

    # this test the function get_games_result with a negative result.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    s_expected = read_txt("edgecases/output_message_calculated_games_result_negative.txt")
    mock_df_game = read_csv("edgecases/q_vw_game_query_negative.csv")

    with patch.object(output_message_calculated_generation, 'snowflake_execute', return_value=mock_df_game):
        s, count = output_message_calculated_generation.get_games_result(sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert count == 2
        assert s == s_expected
    
def test_get_scores_detailed_missing_split(read_yml_as_serie, read_csv,assert_exit):
    
    # this test the function get_scores_detailed with columns without underscore. Must exit the program.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_predict_games = read_csv("edgecases/q_vw_predict_game_query_columns_nounderscores.csv",keep_default_na=False,na_filter=False)
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_predict_games):
        assert_exit(lambda: output_message_calculated_generation.get_scores_detailed(sr_snowflake_account_connect, sr_gameday_output_calculate))

def test_get_scores_global_empty_df(read_csv):
    
    # this test the function get_scores_global with empty dataframe. Must exit the program.
    df_userscores_global = read_csv("edgecases/q_vw_user_scores_global_query_empty.csv")
    mock_df_rank = read_csv("edgecases/q_vw_user_scores_global_query_ranked_empty.csv")
    
    with patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_rank):
        df_result,nb_result = output_message_calculated_generation.get_scores_global(df_userscores_global)

        assert df_result.empty
        assert nb_result == 0
    
def test_get_scores_global_missing_cols(assert_exit):
    
    # this test the function get_scores_global with missing columns. Must exit the program.
    df_userscores_global = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_rank = pd.DataFrame({"USER_NAME":["X"]})

    with patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_rank):
        assert_exit(lambda: output_message_calculated_generation.get_scores_global(df_userscores_global))

def test_get_scores_gameday_empty(read_yml_as_serie, read_csv):
    
    # this test the function get_scores_gameday with empty dataframe. Must return an empty string.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = read_csv("edgecases/q_vw_user_scores_gameday_query_empty.csv")
    mock_df_userscores_gameday_ranked = read_csv("edgecases/q_vw_user_scores_gameday_query_empty.csv")
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            df_result, nb_result = output_message_calculated_generation.get_scores_gameday(sr_snowflake_account_connect, sr_gameday_output_calculate)
            assert df_result.empty
            assert nb_result == 0

def test_get_scores_gameday_missing_column(read_yml_as_serie, read_csv, assert_exit):
    
    # this test the function get_scores_gameday with dataframe having missing columns. Must exit the program.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_userscores_gameday = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_userscores_gameday_ranked = pd.DataFrame({"USER_NAME":["X"]})
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_userscores_gameday), \
        patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_userscores_gameday_ranked):
            assert_exit(lambda: output_message_calculated_generation.get_scores_gameday(sr_snowflake_account_connect, sr_gameday_output_calculate))

def test_get_scores_average_empty_df(read_csv):
    
    # this test the function get_scores_average with empty dataframe. Must return an empty string
    nb_prediction = 4
    df_userscores_global = read_csv("edgecases/q_vw_user_scores_global_query_empty.csv")
    mock_df_rank = read_csv("edgecases/q_vw_user_scores_global_query_ranked_empty.csv")
    
    with patch.object(output_message_calculated_generation.output, 'calculate_and_display_rank', return_value= mock_df_rank):
        s, count, nb_min = output_message_calculated_generation.get_scores_average(nb_prediction, df_userscores_global)
        assert s == ""
        assert nb_min == 2
        assert count == 0

def test_get_scores_average_missing_column( assert_exit):
    
    # this test the function get_scores_average with a dataframe with missing columns. Must exit the program
    nb_prediction = 4
    df_userscores_global = pd.DataFrame({"USER_NAME":["X"]})
    mock_df_rank = pd.DataFrame({"USER_NAME":["X"]})
    
    with patch.object(output_message_calculated_generation.output, 'calculate_and_display_rank', return_value= mock_df_rank):
        assert_exit(lambda: output_message_calculated_generation.get_scores_average(nb_prediction, df_userscores_global))

def test_get_predictchamp_ranking_empty(read_yml_as_serie, read_csv):
    
    # this test the function get_predictchamp_ranking an empty dataframe. Must return an empty dataframe
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_teamscores = read_csv("edgecases/q_vw_team_scores_query_empty.csv")
    mock_df_rank = read_csv("edgecases/q_vw_team_scores_query_ranked_empty.csv")

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_teamscores):
        with patch.object(output_message_calculated_generation.output, "display_rank", return_value=mock_df_rank):
            
            result_df = output_message_calculated_generation.get_predictchamp_ranking(sr_snowflake_account_connect, sr_gameday_output_calculate)
            assert result_df.empty

def test_get_correction_no_rows(read_yml_as_serie, read_csv):
    
    # this test the function get_correction with no rows. Must return an empty string
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_correction = read_csv("edgecases/q_vw_correction_empty.csv")
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_correction):
        result_str, result_count = output_message_calculated_generation.get_correction(sr_snowflake_account_connect, sr_gameday_output_calculate)

        assert result_str == ""
        assert result_count == 0
 
def test_get_list_gameday_missing_columns(assert_exit):
    
    # this test the function get_list_gameday with missing columns. Must exit the program
    df_list_gameday = pd.DataFrame({ "NB_PREDICTION": [3]})
    assert_exit(lambda: output_message_calculated_generation.get_list_gameday(df_list_gameday))

def test_get_list_gameday_empty_dataframe(read_csv):
    
    # this test the function get_list_gameday with empty df. Must return an empty string
    df_list_gameday = read_csv("edgecases/q_vw_gameday_calculated_query_empty.csv")
    result = output_message_calculated_generation.get_list_gameday(df_list_gameday)
    assert result == ""

def test_get_mvp_month_race_figure_empty_df(read_yml_as_serie, read_csv):
    
    # this function test the function get_mvp_month_race_figure with an empty return from query. Must return an empty string
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = read_csv("edgecases/q_vw_user_scores_gameday_mvp_query_empty.csv",quotechar='"')

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_message_calculated_generation.get_mvp_month_race_figure(sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert gameday_month == "__L__MONTH_01__L__"
        assert count == 0
        assert list_user == ""

def test_get_mvp_month_race_figure_invalid_points(read_csv,read_txt, read_yml_as_serie):
    
    # this function test the function get_mvp_month_race_figure with invalid points. Must accept, but should not happen due to test in database which stop the program if not an int.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_month_mvp = read_csv("edgecases/q_vw_user_scores_gameday_mvp_query_invalid_points.csv",quotechar='"')
    expected_str = read_txt("edgecases/output_message_calculated_mvp_race_figures_invalid_points.txt")

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_month_mvp):
        gameday_month, list_user, count = output_message_calculated_generation.get_mvp_month_race_figure(sr_snowflake_account_connect, sr_gameday_output_calculate)
        assert gameday_month == "__L__MONTH_01__L__"
        assert count == 2
        assert list_user == expected_str

def test_list_mvp_month_race_gameday_empty(read_csv,read_yml_as_serie):
    
    # this function test the function list_mvp_month_race_gameday with empty dataframe. Must accept
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_list_gameday = pd.DataFrame(columns=["GAMEDAY", "NB_PREDICTION"])
    expected_str = ""
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_list_gameday):
        str_list_gameday = output_message_calculated_generation.list_mvp_month_race_gameday(sr_snowflake_account_connect,sr_gameday_output_calculate)
        assert str_list_gameday == expected_str

def test_get_mvp_compet_race_figure_missing_key(read_csv,read_yml_as_serie, assert_exit):

    # this function test the function get_mvp_month_race_figure with missing column in sr_gameday_output_calculate. Must exit the program
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("edgecases/sr_gameday_output_calculate_nocompetitionlabel.csv").iloc[0]
    mock_df_compet_mvp = read_csv("q_vw_user_scores_gameday_mvp_query.csv",quotechar='"')
    
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_compet_mvp):
        assert_exit(lambda: output_message_calculated_generation.get_mvp_compet_race_figure(sr_snowflake_account_connect, sr_gameday_output_calculate))

def test_list_mvp_compet_race_gameday_missing_columns(read_csv,read_yml_as_serie, assert_exit):
    
    # this function test the function list_mvp_compet_race_gameday with missing columns. Must exit the program
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_list_gameday = pd.DataFrame(columns=["GAMEDAY"])

    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_list_gameday):
        assert_exit(lambda: output_message_calculated_generation.list_mvp_compet_race_gameday(sr_snowflake_account_connect,sr_gameday_output_calculate))

def test_list_mvp_compet_race_gameday_badtypes(read_csv,read_yml_as_serie, assert_exit):
    
    # this test the function list_mvp_compet_race_gameday with bad type. Must exit the program
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    
    mock_df_list_gameday = read_csv("edgecases/q_vw_gameday_calculated_query_badtype.csv")
    with patch.object(output_message_calculated_generation, "snowflake_execute", return_value=mock_df_list_gameday):
        assert_exit(lambda: output_message_calculated_generation.list_mvp_compet_race_gameday(sr_snowflake_account_connect,sr_gameday_output_calculate))

def test_get_parameters_no_predictchamp(read_csv,read_txt, read_yml_as_serie):

    # this test get_parameters without predictchamp results and rank (empty df_gamepredictchamp). Must not call it
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
    mock_df_gamepredictchamp = read_csv("edgecases/q_vw_game_predictchamp_query_empty.csv")
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
         patch.object(output_message_calculated_generation,"get_predictchamp_result") as mock_predictchamp_result, \
         patch.object(output_message_calculated_generation,"get_predictchamp_ranking") as mock_predictchamp_ranking, \
         patch.object(output_message_calculated_generation,"get_correction", return_value=(mock_str_correction, 2)), \
         patch.object(output_message_calculated_generation,"get_mvp_month_race_figure", return_value=("__L__MONTH_01__L__", mock_str_mvp_month, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_month_race_gameday", return_value=(mock_str_mvp_month_gameday)), \
         patch.object(output_message_calculated_generation,"get_mvp_compet_race_figure", return_value=(mock_str_mvp_compet, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_compet_race_gameday", return_value=(mock_str_mvp_compet_gameday)):

        output_message_calculated_generation.get_parameters(sr_snowflake_account_connect, sr_gameday_output_calculate)
        mock_predictchamp_result.assert_not_called()
        mock_predictchamp_ranking.assert_not_called()

def test_get_parameters_missing_key(read_yml_as_serie, read_csv,read_txt, assert_exit):
    
    # this test the function get_parameters with missing key in sr_gameday_output_calculate. Must exit the program.
    sr_snowflake_account_connect = read_yml_as_serie("snowflake_account_connect.yml")
    sr_gameday_output_calculate = read_csv("edgecases/sr_gameday_output_calculate_noseasonid.csv").iloc[0]

    mock_str_games_result = read_txt("output_message_calculated_games_result.txt")
    mock_df_predict_games = read_csv("output_message_calculated_scores_details.csv", header=[0, 1],keep_default_na=False,na_filter=False)
    mock_df_userscores_global = read_csv("q_vw_user_scores_global_query.csv")
    mock_df_scores_global = read_csv("output_message_calculated_scores_global.csv")
    mock_df_list_gameday = read_csv("q_vw_gameday_calculated_query.csv")
    mock_str_scores_average = read_txt("output_message_calculated_scores_average.txt")
    mock_df_scores_gameday = read_csv("output_message_calculated_scores_gameday.csv")
    mock_str_list_gameday = read_txt("output_message_calculated_list_gameday.txt")
    mock_df_gamepredictchamp = read_csv("edgecases/q_vw_game_predictchamp_query_empty.csv")
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
         patch.object(output_message_calculated_generation,"get_predictchamp_result"), \
         patch.object(output_message_calculated_generation,"get_predictchamp_ranking"), \
         patch.object(output_message_calculated_generation,"get_correction", return_value=(mock_str_correction, 2)), \
         patch.object(output_message_calculated_generation,"get_mvp_month_race_figure", return_value=("__L__MONTH_01__L__", mock_str_mvp_month, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_month_race_gameday", return_value=(mock_str_mvp_month_gameday)), \
         patch.object(output_message_calculated_generation,"get_mvp_compet_race_figure", return_value=(mock_str_mvp_compet, 2)), \
         patch.object(output_message_calculated_generation,"list_mvp_compet_race_gameday", return_value=(mock_str_mvp_compet_gameday)):

        assert_exit(lambda: output_message_calculated_generation.get_parameters(sr_snowflake_account_connect, sr_gameday_output_calculate))

def test_create_message_conditional_blocks(read_csv,read_txt,read_json):
    
    # this test the function create_messages_for_country with a template without parameters. Must return the same result than the template
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
    template = "template"
    translations = read_json("output_gameday_template_translations.json")
    country = "FRANCE"
    forum = 'BI'
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_filename = "result.txt"

    with patch.object(output_message_calculated_generation.output,"define_filename", return_value=mock_filename) as mock_filename, \
         patch.object(output_message_calculated_generation.files_manipulation,"create_txt"):
        
        content, country, forum = output_message_calculated_generation.create_message(param_dict, template, translations, country, forum, sr_gameday_output_calculate)
        
        assert content == template
        assert country == "FRANCE"
        assert forum == 'BI'

def test_process_output_message_no_topics(read_yml_as_serie, read_csv,read_txt, read_json):
    
    # this test the function process_output_message_calculated with no topics. Must run
    context_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'str_output_gameday_calculation_template_france': read_txt("output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_italia': read_txt("output_gameday_calculation_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("output_gameday_template_translations.json")
    }
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = read_csv("edgecases/q_vw_topic_calculate_query_empty.csv")
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

def test_process_output_message_posting_fails(read_yml_as_serie, read_csv,read_txt, assert_exit,read_json):
    
    # this test the function process_output_message_calculated with posting function failing. Must exit the program.
    context_dict = {
        "sr_snowflake_account_connect":  read_yml_as_serie("snowflake_account_connect.yml"),
        'str_output_gameday_calculation_template_france': read_txt("output_gameday_calculation_template_france.txt"),
        'str_output_gameday_calculation_template_italia': read_txt("output_gameday_calculation_template_france.txt"),
        'lst_output_gameday_template_translations': read_json("output_gameday_template_translations.json")
    }
    sr_gameday_output_calculate = read_csv("sr_gameday_output_calculate.csv").iloc[0]
    mock_df_topics = read_csv("q_vw_topic_calculate_query.csv")
    mock_params_retrieved = {'dummy':'param_retrieved'}

    with patch.object(output_message_calculated_generation,"snowflake_execute", return_value=mock_df_topics), \
         patch.object(output_message_calculated_generation,"get_parameters", return_value=mock_params_retrieved), \
         patch.object(output_message_calculated_generation,"multithread_run", side_effect=Exception("boom")):

        assert_exit(lambda: output_message_calculated_generation.process_output_message_calculated(context_dict, sr_gameday_output_calculate))
