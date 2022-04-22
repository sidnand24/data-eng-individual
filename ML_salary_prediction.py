import os
import sys
import pyspark
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import config

number_cores = int(os.environ['NUM_CPUS'])
memory_gb = int(os.environ['AVAILABLE_MEMORY_MB']) // 1024

conf = (
    pyspark.SparkConf()
        .setMaster('local[{}]'.format(number_cores))
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,org.postgresql:postgresql:42.3.2')
        .set('spark.driver.memory', '{}g'.format(memory_gb))
)

sc = pyspark.SparkContext(conf=conf)

sc._jsc.hadoopConfiguration().set('fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', config.AWS_ACCESS_KEY)
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', config.AWS_SECRET_ACCESS_KEY)

spark=pyspark.sql.SparkSession(sc)

# Read in tables from Postgres
url = "jdbc:postgresql://dbnba.cydl0lg4ohrj.eu-west-2.rds.amazonaws.com:5432/dbnba"
properties = {"user": "postgres", "password": config.PG_PASS, "driver": "org.postgresql.Driver"}

df_teams = spark.read.jdbc(url=url, table="nba.teams", properties=properties)
df_players = spark.read.jdbc(url=url, table="nba.players", properties=properties)
df_stats = spark.read.jdbc(url=url, table="nba.stats", properties=properties)
df_tweets = spark.read.jdbc(url=url, table="nba.tweets", properties=properties)

# Calculate total tweet count over week per player
df_tweets = df_tweets.groupBy('player_id').sum('Tweet_count')

# Join tables together to create final ML data for model training
df_ml = df_players.join(df_tweets, df_players.player_id == df_tweets.player_id).select(df_players["*"], df_tweets["sum(Tweet_count)"])

# Select columns desired for models
df_ml = df_ml.select(F.col("player_id"), F.col('position'), F.col('age'), F.col('height'), F.col('weight'),
                     F.col('salary'), F.col('sum(tweet_count)').alias('week_tweets'))

# Obtain desired columns from stats table
df_stats = df_stats.select(F.col('player_id'), F.col("g"), F.col('mp'), F.col('efg_pct'), F.col('trb'), F.col('ast'), F.col('stl'),
                     F.col('blk'), F.col('tov'), F.col('pts'))

# Convert stats to per game metrics
per_game = ['mp', 'trb', 'ast', 'stl', 'blk', 'tov', 'pts']
for x in per_game:
    df_stats = df_stats.withColumn(x, F.round(df_stats[x]/df_stats.g, 1))

# Rename columns to per game
df_stats = df_stats.select(F.col('player_id'), F.col("mp").alias('MPG'), F.col('efg_pct'), F.col('trb').alias('TRBPG'),
                           F.col('ast').alias('APG'), F.col('stl').alias('SPG'), F.col('blk').alias('BPG'),
                           F.col('tov').alias('TOPG'), F.col('pts').alias('PPG'))

df_ml = df_ml.join(df_stats, ['player_id'])

# Fill missing values with mean for each column
def fill_missing(df, exclude=set()):
    stats = df.agg(*(F.avg(c).alias(c) for c in df.columns if c not in exclude))
    return df.na.fill(stats.first().asDict())

df_ml = fill_missing(df_ml, ["player_id", "position"])

# Convert column types
df_ml = df_ml.withColumn("week_tweets", df_ml["week_tweets"].cast(IntegerType()))
df_ml = df_ml.withColumn("efg_pct", df_ml["efg_pct"].cast(DoubleType()))
df_ml = df_ml.withColumn("efg_pct", F.round(df_ml["efg_pct"], 2))

df_ml = df_ml.drop(df_ml.player_id)

# Convert position column to numerical column after encoding
indexer = StringIndexer(inputCol='position', outputCol='posNum')
indexd_data=indexer.fit(df_ml).transform(df_ml)

encoder = OneHotEncoder(inputCol='posNum', outputCol = 'posVec')
onehotdata = encoder.fit(indexd_data).transform(indexd_data)

# Create feature vector for models
assembler1 = VectorAssembler(
 inputCols=["age", "height", "week_tweets", "MPG", "efg_pct", "TRBPG", "APG", "SPG", "BPG", "TOPG", "PPG", "posVec"],
 outputCol="features")
outdata1 = assembler1.transform(onehotdata)

df = outdata1.select('features', 'salary')
df = df.withColumnRenamed("salary","label")

# Save pre-processed data into table with features and label
cwd = os.getcwd()
df.repartition(1).write.parquet(cwd+"/processed_data.parquet")

