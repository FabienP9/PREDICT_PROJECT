'''
This tests file concern all functions in the messages_posting_process_bi.
It units test the happy path for each function
'''

from unittest.mock import MagicMock, patch

from src.predict_core.forums_interaction.forums_interaction_bi import messages_posting_process_bi

def test_post_edit_message_bi(read_csv):

    # this test the function post_edit_message_bi
    topic_row = read_csv("q_topics_query.csv").iloc[0]
    message_content = "Hello BI forum!"

    with patch.object(messages_posting_process_bi,"login_and_post_message_bi") as mock_post:

        messages_posting_process_bi.post_edit_message_bi(topic_row, message_content)
        assert mock_post.call_count == 2

def test_login_to_bi():

    #This test the function login_to_bi
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
        Déconnexion
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

        # Should not raise SystemExit
        messages_posting_process_bi.login_to_bi(mock_session, login_url)

def test_post_to_bi():
    
    #this test the function post_to_bi
    message_content = "fake message to post"
    post_url = "fakeposturl"
    sid = "sid123"
    is_to_edit = 0

    mock_post_get = """
        <form>
            <input name="sid" value="sid123">
            <input name="form_token" value="token123">
            <input name="creation_time" value="time123">
        </form>
        Déconnexion
    """

    mock_post_post = """
        <form>
            <input name="sid" value="sid123">
            <input name="form_token" value="token123">
            <input name="creation_time" value="time123">
        </form>
        Déconnexion
    """

    with patch.object(messages_posting_process_bi.requests,"Session") as mock_session:
        
        mock_sess_instance = MagicMock()
        mock_session.return_value.__enter__.return_value = mock_sess_instance
        mock_sess_instance.get.return_value.text = mock_post_get
        mock_sess_instance.post.return_value.text = mock_post_post

        # Should not raise SystemExit
        messages_posting_process_bi.post_to_bi(message_content, mock_session, post_url, sid, is_to_edit)

def test_login_and_post_message_bi(read_csv):
    # this test the function login_and_post_message_bi
    topic_row = read_csv("q_topics_query.csv").iloc[0]
    message_content = "Test message"
    is_to_edit = 1

    mock_login_successful = True
    mock_sid = "sid123"
    mock_post_successful = True

    # Mock os variables
    with patch.dict(
        "os.environ",
        {
            "BI_URL": "https://fakeforum.com"
        },
    ), \
    patch.object(messages_posting_process_bi,"login_to_bi", return_value=(mock_login_successful, mock_sid)), \
    patch.object(messages_posting_process_bi,"post_to_bi", return_value=mock_post_successful):

        # Should not raise SystemExit
        messages_posting_process_bi.login_and_post_message_bi(topic_row, message_content, is_to_edit)

