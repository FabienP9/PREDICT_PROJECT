'''
    The purpose of this module is to process imgbb image storage website actions.
    It pushes capture on the website, and get the url
'''
import os
import requests

import config

@config.exit_program(log_filter=lambda args: dict(args))
@config.retry_function(log_filter=lambda args: dict(args))
def push_capture_online(image_path: str) -> str:

    '''
        Sends an image of a capture on ImgBB website
        Input:
            image_path(str): The path of the capture on the local environment
        Returns:
            The url of the capture online  
        Raises:
            Retry 3 times and exits the program if error with ImgBB (using decorators)
    '''

    # we send online using the ImgBB API
    api_key = os.getenv('IMGBB_API_KEY')
    with open(image_path, 'rb') as file:
        url = "https://api.imgbb.com/1/upload"
        payload = {
            "key": api_key,
        }
        files = {
            "image": file,
        }
        response = requests.post(url, data=payload, files=files)
        
    image_url = response.json()['data']['url']
    return image_url