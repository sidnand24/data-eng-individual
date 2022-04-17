import os
import sys
import pyspark
import findspark
findspark.init()
from pyspark.sql import functions as F
from pyspark.sql.window import Window  
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


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


# Read parquet files from data collection
df_rosters = spark.read.parquet("s3a://msin0166nbadata/raw_collected/df_rosters/")
df_tweets = spark.read.parquet("s3a://msin0166nbadata/raw_collected/df_tweets/")
df_stats = spark.read.parquet("s3a://msin0166nbadata/raw_collected/df_stats/")
df_teams = spark.read.parquet("s3a://msin0166nbadata/raw_collected/df_teams/")


# ----------------------------
# teams table
# ----------------------------

# Lower case team name for ease of joining tables later
df_teams = df_teams.withColumn("full_name", F.lower(F.col("full_name")))

# Rename id column to team_id for primary key
df_teams = df_teams.withColumnRenamed("id", "team_id")

# Convert number columns to integers
df_teams = df_teams.withColumn("team_id", df_teams["team_id"].cast(IntegerType()))
df_teams = df_teams.withColumn("year_founded", df_teams["year_founded"].cast(IntegerType()))

# ----------------------------
# players table
# ----------------------------

# Clean team name column so matches with the teams table for joining
df_rosters = df_rosters.withColumn("team", F.regexp_replace('team', "[-]", " "))
df_rosters = df_rosters.withColumn("team", F.regexp_replace('team', "la clippers", "los angeles clippers"))

# Split name column into first and last name on the first space
df_rosters = df_rosters.withColumn('first_name', F.split('Name',"(?<=^[^\s]*)\\s")[0]).withColumn('last_name', F.split('Name',"(?<=^[^\s]*)\\s")[1])

# Create an index column for the primary key
w = Window.orderBy("Name") 
df_rosters = df_rosters.withColumn("player_id", F.row_number().over(w))

# Join teams table to add team_id instead of team name
df_rosters = df_rosters.join(df_teams, df_rosters.team == df_teams.full_name).select(df_rosters["*"], df_teams["team_id"])

# Convert number columns to integers
cols = ['Age', 'HT', 'WT', 'Salary']
for col_name in cols:
    df_rosters = df_rosters.withColumn(col_name, df_rosters[col_name].cast(IntegerType()))
    
# ----------------------------
# stats table
# ----------------------------

# Join player table to add player_id instead of name
df_stats = df_stats.join(df_rosters, df_stats.Player == df_rosters.Name).select(df_stats["*"], df_rosters["player_id"])

# Replace percentage sign in column names
for name in df_stats.schema.names:
    df_stats = df_stats.withColumnRenamed(name, name.replace('%', '_PCT'))

# Convert columns to decimals and integers
intcols = ['G', 'GS', 'MP', 'FG', 'FGA', '3P', '3PA', '2P', '2PA', 'FT', 'FTA', 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
realcols = ['FG_PCT', '3P_PCT', '2P_PCT', 'eFG_PCT', 'FT_PCT']

for x in intcols:
    df_stats = df_stats.withColumn(x, df_stats[x].cast(IntegerType()))
    
for y in realcols:
    df_stats = df_stats.withColumn(y, df_stats[y].cast(FloatType()))
    
# ----------------------------
# tweets table
# ----------------------------

# Join player table to add player_id instead of name
df_tweets = df_tweets.join(df_rosters, df_tweets.Player == df_rosters.Name).select(df_tweets["*"], df_rosters["player_id"])

# Create an index column for tweet_id
w = Window.orderBy("Player") 
df_tweets = df_tweets.withColumn("tweet_id", F.row_number().over(w))


# Reoder tables for Postgres and save to s3

df_rosters = df_rosters.select(F.col("player_id"), F.col('first_name'), F.col('last_name'), F.col('team_id'), F.col('POS').alias('position'), F.col('Age').alias('age'),
                 F.col('HT').alias('height'), F.col('WT').alias('weight'), F.col('College').alias('college'), F.col('Salary').alias('salary'))

df_stats = df_stats.select([df_stats.columns[-1]] + df_stats.columns[4:-1])

df_tweets = df_tweets.select(F.col("tweet_id"), F.col('player_id'), F.col('Date').alias('tweet_date'), F.col('tweet_count'))

### Store tables in S3
df_teams.write.parquet("s3a://msin0166nbadata/postgres_tables/teams_pg/")
df_rosters.write.parquet("s3a://msin0166nbadata/postgres_tables/players_pg/")
df_stats.write.parquet("s3a://msin0166nbadata/postgres_tables/stats_pg/")
df_tweets.write.parquet("s3a://msin0166nbadata/postgres_tables/tweets_pg/")

