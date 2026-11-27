
from src.predict_core.forums_interaction import messages_posting_process

def test_post_edit_message_no_message_to_edit_posts_main_only():
    
    #this test the post_edit_message function, without edit message.
    calls = []

    def fake_post_function(topic_row, message, is_to_edit, is_result):
        calls.append((message, is_to_edit, is_result))

    original_dict = messages_posting_process.messages_post_functions
    messages_posting_process.messages_post_functions = {"forum_x": fake_post_function}

    try:
        topic_row = {
        "FORUM_SOURCE": "forum_x",
        "IS_FOR_PREDICT": 1,
        "IS_FOR_RESULT": 0,
        "MESSAGE_NUMBER_TO_EDIT": float("nan"),
        }
    
        messages_posting_process.post_edit_message(topic_row, "main message", "small message")

        assert calls == [("main message", 0, 0)]  # one new post, small message never posted
    finally:
        messages_posting_process.messages_post_functions = original_dict