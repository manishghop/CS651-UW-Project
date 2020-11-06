import findspark
#
# findspark.init(r'/usr/local/spark-2.4.7-bin-hadoop2.7')
findspark.init(r'C:\Users\manis\Documents\spark-3.0.1-bin-hadoop2.7')
import re
import subprocess
import sys
import time
from collections import namedtuple

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.streaming import StreamingContext


# Function to clean tweet
def clean_tweet(tweet):
    return ' '.join(re.sub('(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)', ' ', str(tweet)).split())


def analyze_sentiment(tweet: str):
    try:
        from textblob import TextBlob
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'textblob'])
    finally:
        from textblob import TextBlob
    sentiment = TextBlob(clean_tweet(tweet)).sentiment
    polarity = 0
    if sentiment.polarity > 0:
        polarity = 1
    elif sentiment.polarity < 0:
        polarity = -1
    return polarity, str(sentiment.subjectivity)


def main():
    # Creating the Spark context
    sc = SparkContext(master='local[2]', appName='TwitterSentAnalysis')
    sc.setLogLevel('ERROR')
    
    # Creating the streaming context with batch interval of 5 sec
    ssc = StreamingContext(sparkContext=sc, batchDuration=5)
    ssc.checkpoint('checkpoints')
    
    sqlContext = SparkSession.builder.getOrCreate()
    
    tweets = ssc.socketTextStream(hostname='localhost', port=3333)
    
    tweets.cache()
    
    def save_sentiments(rdd):
        sentiments = rdd.toDF()
        sentiments.coalesce(1).write.mode('append').format('json').save(r'sentiments')
        sentiments.createOrReplaceTempView('sentiments')
    
    def user_id_group(rdd):
	    rdd.groupbyKey(lambda x:x['user']['id'])
		
    # Transforming using basic spark functions
    sentiments_fields = ('tweet', 'polarity', 'subjectivity')
    sentiments_obj = namedtuple('sentiment', sentiments_fields)
    tweets.map(lambda tweet: (tweet, *analyze_sentiment(tweet))) \
        .map(lambda p: sentiments_obj(p[0], p[1], p[2])).filter(lambda x:len(x[0])>0).filter(lambda x: not x[0].isdigit()) \
        .window(60, 15) \
        .foreachRDD(save_sentiments)
    
    #tweets.foreachRDD(user_id_group)
    tweets.pprint()
	
    tag_fields = ('hashtag', 'count')
    tag_obj = namedtuple('tag', tag_fields)
    
    def save_hashtags(rdd):
        hashtags = rdd.sortBy(lambda x: x[1], ascending=False).toDF()
        hashtags.coalesce(1).write.mode('append').format('json').save(r'hashtags')
        hashtags.limit(10).createOrReplaceTempView('hashtags')
        
    tweets.flatMap(lambda tweet: tweet.split(' ')) \
        .filter(lambda word: word.startswith('#') and word is not '#') \
        .map(lambda hashtag: (hashtag.replace('#', ''), 1)) \
        .reduceByKeyAndWindow(lambda counts, x: counts + x, lambda counts, x: counts - x, 60, 15) \
        .map(lambda p: tag_obj(p[0], p[1])) \
        .foreachRDD(save_hashtags)
    
    ssc.start()
    
    while True:
        print('Waiting for another 15 Seconds.....')
        time.sleep(15)
        try:
            top_10_tweets = sqlContext.sql('select * from hashtags')
            top_10_df = top_10_tweets.toPandas()
            
            print('------------------------------- \n')
            print(top_10_df)
            
            pos_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = 1')
            neu_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = 0')
            neg_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = -1')
            
            pos_df = pos_sentiment.toPandas()
            neu_df = neu_sentiment.toPandas()
            neg_df = neg_sentiment.toPandas()
            
            total_sentiment = pos_df['count(tweet)'].iloc[0] + neg_df['count(tweet)'].iloc[0] + \
                              neu_df['count(tweet)'].iloc[0]
            
            percent_pos = (pos_df['count(tweet)'].iloc[0] / total_sentiment) * 100
            print('Percentage of Positive Sentiment = {} %'.format(round(percent_pos, 2)))
            
            percent_neu = (neu_df['count(tweet)'].iloc[0] / total_sentiment) * 100
            print('Percentage of Neutral Sentiment = {} %'.format(round(percent_neu, 2)))
            
            percent_neg = (neg_df['count(tweet)'].iloc[0] / total_sentiment) * 100
            print('Percentage of Negative Sentiment = {} %'.format(round(percent_neg, 2)))
        except AnalysisException:
            print('SQL data is not ready yet...')
    
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
