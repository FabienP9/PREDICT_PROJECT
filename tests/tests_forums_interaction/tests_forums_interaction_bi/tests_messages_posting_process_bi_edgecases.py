'''
This tests file concern all functions in the messages_posting_process_bi.
It units test unexpected path
'''
from unittest.mock import MagicMock, patch

from src.predict_core.forums_interaction.forums_interaction_bi import messages_posting_process_bi

def test_login_to_bi_fails():

    #This test the function login_to_bi failing. Must return False
    login_url = "fakeloginurl"
    mock_login_get = """
        <form>
            <input name="sid" value="sid123">
            <input name="form_token" value="token123">
            <input name="creation_time" value="time123">
        </form>
        Déconnexion
    """
    mock_login_post = """
        Login failed
    """

    with patch.dict(
        "os.environ",
        {
            "BI_URL": "https://fakeforum.com",
            "BI_USERNAME": "fake_user",
            "BI_PASSWORD": "fake_pass", # NOSONAR
        },
    ), patch.object(messages_posting_process_bi.requests,"Session") as mock_session:
        
        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance
        mock_sess_instance.get.return_value.text = mock_login_get
        mock_sess_instance.post.return_value.text = mock_login_post

        # Should return False
        login_successful, sid = messages_posting_process_bi.login_to_bi(mock_session, login_url)
        assert not login_successful 
        assert sid == ""

def test_login_and_post_message_bi_timesout(read_csv, assert_exit):
    # this test the function login_and_post_message_bi with timesout
    topic_row = read_csv("q_topics_query.csv").iloc[0]
    message_content = "Test message"
    is_to_edit = 1

    mock_login_successful = False
    mock_sid = ""
    mock_post_successful = False

    # Mock os variables
    with patch.dict(
        "os.environ",
        {
            "BI_URL": "https://fakeforum.com"
        },
    ), \
    patch.object(messages_posting_process_bi,"login_to_bi", return_value=(mock_login_successful, mock_sid)), \
    patch.object(messages_posting_process_bi,"post_to_bi", return_value=mock_post_successful), \
    patch.object(messages_posting_process_bi.var, "TIME_MESSAGE_WAIT", 1):

        # Should raise SystemExit
        assert_exit(lambda: messages_posting_process_bi.login_and_post_message_bi(topic_row, message_content, is_to_edit))

def test_post_message_bi_times_out(read_csv, assert_exit):
    # this test the function post_message_bi with timesout
    topic_row = read_csv("q_topics_query.csv").iloc[0]
    topic_row['MESSAGE_NUMBER_TO_EDIT'] = 123  # E
    
    fake_login_html = """
        <input name="sid" value="1"/>
        <input name="form_token" value="2"/>
        <input name="creation_time" value="3"/>
    """

    mock_session = MagicMock()
    mock_session.get.return_value.text = fake_login_html
    mock_session.post.return_value.text = "login failed"  # This causes login failure

    session_ctx = MagicMock()
    session_ctx.__enter__.return_value = mock_session
    session_ctx.__exit__.return_value = None

    with patch("requests.Session", return_value=session_ctx), \
         patch("os.getenv", return_value="http://fake-forum"), \
         patch.object(messages_posting_process_bi.var, "TIME_MESSAGE_WAIT", 1):
        
        assert_exit(lambda: messages_posting_process_bi.post_edit_message_bi(topic_row, "timeout test"))
