import os
import sys
import pyspark
import findspark
findspark.init()

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col, when

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import requests
import pandas as pd
from nba_api.stats.static import teams

# import module for access keys
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


# API query to get team info data
squads = teams.get_teams()

# pandas dataframe as limited number of rows and preserves order of columns
df = pd.DataFrame(squads)

# Convert into Spark DataFrame
df_teams = spark.createDataFrame(df)

# Upload pyspark DataFrame as Parquet format into S3 Bucket
df_teams.write.parquet("s3a://msin0166nbadata/raw_collected/df_teams/")

