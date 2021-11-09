from requests import post
from random import randint

channel_to_post_to = "x-minute-city"

greetings = ["Kia ora!", "Great news!"]  


def post_message_to_slack(message, greet=True):
    """
    Function to post a message to a Slack channel.
    
    :param message: A string to post to the slack channel.
    :param greet: Default: True. If True, post a cheerful greeting before the message.
    :returns: None.

    """
    
    if greet:
        greeting = greetings[randint(0, max(len(greetings) - 1, 0))] + " "
    else:
        greeting = ""

    post('https://slack.com/api/chat.postMessage', {
        'token': open('src/slack_token.txt', 'r').read().strip('\n'),
        'channel': '#' + channel_to_post_to,
        'text': greeting + message
    })