import os
import sys
import pyspark
import findspark
findspark.init()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import tweepy


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


df_rosters = spark.read.parquet("s3a://msin0166nbadata/df_rosters/")


ballers = list(map(lambda row: row[0], df_rosters.rdd.takeSample(False, 100, seed = 100)))


client = tweepy.Client(bearer_token = config.TwitterBearerToken)


tweets_dict = {}
# create query which ignores retweets to get count of tweets per day
for player in ballers:
    query = str(player) + " -is:retweet"
    counts = client.get_recent_tweets_count(query = query, granularity = 'day')
    
    tweets = []
    for x in counts.data:
        tweets.append(x['tweet_count'])
    
    tweets_dict[player] = sum(tweets)
    
tweets_dict



