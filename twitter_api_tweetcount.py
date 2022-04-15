import os
import sys
import pyspark
import findspark
findspark.init()
from pyspark.sql import functions as F 
from pyspark.sql.types import IntegerType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import tweepy
import pandas as pd
import random


import config

number_cores = int(os.environ['NUM_CPUS'])
memory_gb = int(os.environ['AVAILABLE_MEMORY_MB']) // 1024

conf = (
    pyspark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1')
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)

sc = pyspark.SparkContext(conf=conf)

sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', config.AWS_ACCESS_KEY)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', config.AWS_SECRET_ACCESS_KEY)

spark=pyspark.sql.SparkSession(sc)


# Read roster file to get player names
df_rosters = spark.read.parquet("s3a://msin0166nbadata/df_rosters/")


# Random sample of 100 players
ballers = list(map(lambda row: row[0], df_rosters.rdd.takeSample(False, 100, seed = 100)))


# Using Tweepy with bearer token
client = tweepy.Client(bearer_token = config.TwitterBearerToken)

player_dict = {}

# create query which ignores retweets to get count of tweets per day
for player in ballers:
    query = str(player) + " -is:retweet"
    counts = client.get_recent_tweets_count(query = query, granularity = 'day')
    
    tweets =[]
    for x in counts.data:
        tweets.append([x['start'], x['tweet_count']])
    player_dict[player] = tweets
    
    
# Create dataframe of player and tweet count per day
tweet_df = pd.DataFrame(player_dict.items(), columns=['Player','Tweet'])
tweet_df = tweet_df.explode('Tweet')

tweet_df[['Date','Tweet_count']] = pd.DataFrame(tweet_df["Tweet"].tolist(), index= tweet_df.index)
tweet_df.drop(columns=['Tweet'],inplace=True)


# Work out players that dont have tweet count data collected
total = df_rosters.select('Name').rdd.flatMap(lambda x: x).collect()
diff = list(set(total).difference(ballers))


# get the days each player has data for
days = tweet_df.head(8)
unique_day = list(days.Date.unique())


# create new row entrys for spark dataframe for players not in sample
row = []
for x in diff:
    for day in unique_day:
        entry = (x, day, '')
        row.append(entry)


columns = ['Player', 'Date', 'Tweet_count']
new_entries = spark.createDataFrame(row, columns)
tweets_spark = spark.createDataFrame(tweet_df)

# Join spark dataframes
all_tweets = tweets_spark.union(new_entries)


# Fill empty strings with missing values
all_tweets = all_tweets.select([F.when(F.col(c)== "", None).otherwise(F.col(c)).alias(c) for c in all_tweets.columns])

all_tweets = all_tweets.withColumn("Tweet_count", all_tweets["Tweet_count"].cast(IntegerType()))

# Fill missing values with random numbers to augment data
all_tweets = all_tweets.withColumn('Tweet_count', F.coalesce(F.col('Tweet_count'), (F.round(F.rand()*100))))

all_tweets = all_tweets.withColumn("Tweet_count", all_tweets["Tweet_count"].cast(IntegerType()))


# Upload pyspark DataFrame as Parquet format into S3 Bucket
all_tweets.write.parquet("s3a://msin0166nbadata/df_tweets/")
