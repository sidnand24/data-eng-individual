import os
import sys
import pyspark
import findspark
findspark.init()
from pyspark.sql import functions as F 
from pyspark.sql.types import IntegerType

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

# Read parquet files from S3 postgres folder
teams_pg = spark.read.parquet("s3a://msin0166nbadata/postgres_tables/teams_pg/")
players_pg = spark.read.parquet("s3a://msin0166nbadata/postgres_tables/players_pg/")
stats_pg = spark.read.parquet("s3a://msin0166nbadata/postgres_tables/stats_pg/")
tweets_pg = spark.read.parquet("s3a://msin0166nbadata/postgres_tables/tweets_pg/")

# Add tables into Postgres database
mode = "append"
url = "jdbc:postgresql://dbnba.cydl0lg4ohrj.eu-west-2.rds.amazonaws.com:5432/dbnba"
properties = {"user": "postgres", "password": config.PG_PASS, "driver": "org.postgresql.Driver"}

teams_pg.write.jdbc(url=url, table="nba.teams", mode=mode, properties=properties)

players_pg.write.jdbc(url=url, table="nba.players", mode='append', properties=properties)

stats_pg.write.jdbc(url=url, table="nba.stats", mode='append', properties=properties)

tweets_pg.write.jdbc(url=url, table="nba.tweets", mode='append', properties=properties)