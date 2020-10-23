import json
from tweepy import OAuthHandler, Stream, API
from tweepy.streaming import StreamListener
import socket


class TweetsListener(StreamListener):
    def __init__(self, api, conn):
        super(StreamListener, self).__init__()
        self.api = api
        self.socket = conn
    
    def on_data(self, raw_data):
        try:
            msg = extract_tweet_text(raw_data)
            print(msg + '\n')
            self.socket.send(str(msg).encode('utf-8'))
        except Exception as e:
            print('Error on_data: %s' % str(e))
        return True
    
    def on_error(self, status_code):
        print('Error in producer')
        return True
    
    def on_timeout(self):
        return True


def extract_tweet_text(raw_data):
    data = json.loads(raw_data)
    if 'retweeted_status' in data and 'extended_tweet' in data['retweeted_status']:
        return data['retweeted_status']['extended_tweet']['full_text']
    elif 'extended_status' in data:
        return data['extended_status']['full_text']
    return data['text']


def main():
    access_token = '1314295814802472960-1Ko0yTYipUKpmHYe9KRmc5Vsm9NDMr'
    access_secret = 'O6VwLeq18J64EupW3bOupe8wDNESO7teSAuTOJL3mXpiY'
    consumer_key = 'oPBtnqOvoZUSHWrLIA1SLn1Eh'
    consumer_secret = 'CrI5XRUPvRR7fcPWfNLI8JS9DGwmWKw8h6OyjxeO4gF9inZjFw'
    
    # Initializing the port and host
    host = '0.0.0.0'
    port = 3333
    address = (host, port)
    
    # Initializing the socket
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(address)
    server_socket.listen(5)
    
    print('Listening for client...')
    conn, address = server_socket.accept()
    
    print('Connected to Client at ' + str(address))
    
    # Authenticating
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = API(auth)
    
    # Establishing the twitter streams
    twitter_stream = Stream(auth, TweetsListener(api, conn), tweet_mode='extended_tweet')
    twitter_stream.filter(track=['Trump', 'Biden'], is_async=True, languages=['en'])


if __name__ == '__main__':
    main()
