import findspark
#
# findspark.init(r'/usr/local/spark-2.4.7-bin-hadoop2.7')
findspark.init(r'C:\Users\manis\Documents\spark-3.0.1-bin-hadoop2.7')
import re
import subprocess
import sys
import time
from collections import namedtuple
import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from pyspark.streaming import StreamingContext

import os
from os import listdir
from os.path import isfile, join
print(os.getcwd())
mypath=os.getcwd()
onlyfiles = [f for f in listdir(mypath+"\\sentiments") if f!="_SUCCESS" and f!="_temporary"]



# Function to remove emojis
def deEmojify(text):
    regrex_pattern = re.compile(pattern = "["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           "]+", flags = re.UNICODE)
    return regrex_pattern.sub(r'',text)


# Function to clean tweet
def clean_tweet(tweet):
    tweet=deEmojify(tweet)
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
    #ssc = StreamingContext(sparkContext=sc, batchDuration=5)
    #ssc.checkpoint('checkpoints')
    
    sqlContext = SparkSession.builder.getOrCreate()
    #tweets=sc.textFile('')
    #tweets = ssc.socketTextStream(hostname='localhost', port=3333)
    
    #tweets.cache()
    
    def save_sentiments(rdd):
        sentiments = rdd.toDF()
        sentiments.coalesce(1).write.mode('append').format('json').save(r'sentiments')
        sentiments.createOrReplaceTempView('sentiments')
    
    def user_id_group(rdd):
	    rdd.groupbyKey(lambda x:x['user']['id'])
		
    # Transforming using basic spark functions
    sentiments_fields = ('tweet', 'polarity', 'subjectivity')
    sentiments_obj = namedtuple('sentiment', sentiments_fields)
    #tweets.map(lambda tweet: (tweet, *analyze_sentiment(tweet))) \
    #    .map(lambda p: sentiments_obj(p[0], p[1], p[2])).filter(lambda x:len(x[0])>0).filter(lambda x: not x[0].isdigit()) \
    #    .window(60, 15) \
    #    .foreachRDD(save_sentiments)
    
    #tweets.foreachRDD(user_id_group)
    #tweets.pprint()
	
    tag_fields = ('hashtag', 'count')
    tag_obj = namedtuple('tag', tag_fields)
    
	
	
    def sentiment_calculator(f):
	    df=sqlContext.read.json(f)
	    print(df)
	    df.printSchema()
	    df.createOrReplaceTempView("sentiments")
	    ans=sqlContext.sql("select * from sentiments")
	    ans.show()
	    pos_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = 1')
	    neu_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = 0')
	    neg_sentiment = sqlContext.sql('select count(tweet) from sentiments where polarity = -1')
	
	    pos_df = pos_sentiment.toPandas()
	    neu_df = neu_sentiment.toPandas()
	    neg_df = neg_sentiment.toPandas()
	    pos_df.head()
	    total_sentiment = pos_df['count(tweet)'].iloc[0] + neg_df['count(tweet)'].iloc[0] + neu_df['count(tweet)'].iloc[0]
	    #print("Length of file is ",total_sentiment)
	    
	    percent_pos = (pos_df['count(tweet)'].iloc[0] / total_sentiment) * 100
	    
	    percent_neu = (neu_df['count(tweet)'].iloc[0] / total_sentiment) * 100
	    
	    percent_neg = (neg_df['count(tweet)'].iloc[0] / total_sentiment) * 100
	    return percent_pos,percent_neg,percent_neu,total_sentiment
    
    def save_hashtags(rdd):
        hashtags = rdd.sortBy(lambda x: x[1], ascending=False).toDF()
        hashtags.coalesce(1).write.mode('append').format('json').save(r'hashtags')
        hashtags.limit(10).createOrReplaceTempView('hashtags')

    positive=0
    negative=0
    nuetral=0
	
    states=["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina", "North Dakota",  "Ohio", "Oklahoma", "Oregon",  "Pennsylvania",  "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",  "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"]
    states_result={i:[] for i in states}	
    indices={0: 'Alabama', 1: 'Alaska', 2: 'Arizona', 3: 'Arkansas', 4: 'California', 5: 'Colorado', 6: 'Connecticut', 7: 'Delaware', 8: 'Florida', 9: 'Georgia', 10: 'Hawaii', 11: 'Idaho', 12: 'Illinois', 13: 'Indiana', 14: 'Iowa', 15: 'Kansas', 16: 'Kentucky', 17: 'Louisiana', 18: 'Maine', 19: 'Maryland', 20: 'Massachusetts', 21: 'Michigan', 22: 'Minnesota', 23: 'Mississippi', 24: 'Missouri', 25: 'Montana', 26: 'Nebraska', 27: 'Nevada', 28: 'New Hampshire', 29: 'New Jersey', 30: 'New Mexico', 31: 'New York', 32: 'North Carolina', 33: 'North Dakota', 34: 'Ohio', 35: 'Oklahoma', 36: 'Oregon', 37: 'Pennsylvania', 38: 'Rhode Island', 39: 'South Carolina', 40: 'South Dakota', 41: 'Tennessee', 42: 'Texas', 43: 'Utah', 44: 'Vermont', 45: 'Virginia', 46: 'Washington', 47: 'West Virginia', 48: 'Wisconsin', 49: 'Wyoming'}
    for i,f in enumerate(onlyfiles):
	    posi,nega,nue,total_tweets=sentiment_calculator("./sentiments/"+f)
	    states_result[indices[i]]=[posi,nega,nue,total_tweets]
	    positive+=posi
	    negative+=nega
	    nuetral+=nue
	
    f=open('states.txt','w+')
    for i in states_result:
	    f.write(str(i)+str(states_result[i])+"\n")
    f.close()
	
    percent_pos=positive/len(onlyfiles)
    percent_neg=negative/len(onlyfiles)
    percent_neu=nuetral/len(onlyfiles)
    print('Percentage of Positive Sentiment = {} %'.format(round(percent_pos, 2)))
    print('Percentage of Neutral Sentiment = {} %'.format(round(percent_neu, 2)))	
    print('Percentage of Negative Sentiment = {} %'.format(round(percent_neg, 2)))
    """
    while True:
        print('Waiting for another 15 Seconds.....')
        #time.sleep(15)
        print(sentiments)
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
            pos_df.head()
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
    
    #ssc.awaitTermination()
   """


if __name__ == '__main__':
    main()
