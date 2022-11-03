# -*- coding: utf-8 -*-

import requests

class Slack:
    """
    Slackにメッセージを送信
    """
    def __init__(self, token):
        self.token = token
    
    def send_message(self, channel, message):
        """
        指定したSlackチャンネルにメッセージを送信
        """
        url = 'https://slack.com/api/chat.postMessage'
        headers = {"Authorization": "Bearer "+self.token}
        data  = {
        'channel': channel,
        'text': message
        }
        r = requests.post(url, headers=headers, data=data)
        print('Sent a message to #{}'.format(channel))
