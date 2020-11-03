import json
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener


class TweetsListener(StreamListener):
    def __init__(self, api):
        super(StreamListener, self).__init__()
        self.api = api
    
    def on_data(self, raw_data):
        try:
            tweet = extract_tweet_text(raw_data)
            with open('tweets.txt', 'a') as file:
                file.write(json.dumps(tweet)+'\n')
        except Exception as e:
            print('Error on_data: %s' % str(e))
            print(raw_data)
        return True
    
    def on_error(self, status_code):
        print('Error in producer')
        return True
    
    def on_timeout(self):
        return True


def extract_tweet_text(raw_data):
    data = json.loads(raw_data)
    if 'lang' in data and data['lang'] != 'en':
        raise Exception('Wrong tweet language')
    tweet = {}
    if 'retweeted_status' in data and 'extended_tweet' in data['retweeted_status']:
        tweet['text'] = data['retweeted_status']['extended_tweet']['full_text']
    elif 'extended_status' in data:
        tweet['text'] = data['extended_status']['full_text']
    tweet['text'] = data['text']
    tweet['user_id'] = data['user']['id']
    return tweet


def main():
    access_token = '1314295814802472960-1Ko0yTYipUKpmHYe9KRmc5Vsm9NDMr'
    access_secret = 'O6VwLeq18J64EupW3bOupe8wDNESO7teSAuTOJL3mXpiY'
    consumer_key = 'oPBtnqOvoZUSHWrLIA1SLn1Eh'
    consumer_secret = 'CrI5XRUPvRR7fcPWfNLI8JS9DGwmWKw8h6OyjxeO4gF9inZjFw'
    
    # Authenticating
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = API(auth)
    
    # Establishing the twitter streams
    stream_track = ['Trump', 'Biden', 'US Elections 2020', '2020election', 'PresidentialElection']
    us_bbox = [-171.791110603, 18.91619, -66.96466, 71.3577635769]

    twitter_stream = Stream(auth, TweetsListener(api), tweet_mode='extended_tweet')
    twitter_stream.filter(track=stream_track, is_async=True, languages=['en'], locations=us_bbox)


if __name__ == '__main__':
    main()
