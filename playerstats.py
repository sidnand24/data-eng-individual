import os
import sys
import pyspark
import findspark
findspark.init()
from pyspark.sql.functions import col, when

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

import requests
from bs4 import BeautifulSoup
import pandas as pd

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


# Include header to identify yourself to website when scraping
headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
    }

# season to scrape
season = "2022"

# Use BeautifulSoup to scrape website
url = "https://www.basketball-reference.com/leagues/NBA_"+season+"_totals.html"
r = requests.get(url, headers=headers)
text = r.text
soup = BeautifulSoup(text)


table = soup.find_all(class_='full_table')
# Get column names
columns = [th.getText() for th in soup.find_all('tr', limit=2)[0].find_all('th')[1:]]


# Create dataframe for player stats
stats = []
for i in range(len(table)):
    row = []
    for x in table[i].find_all("td"):
        row.append(x.text)
        
    stats.append(row)
    
df = pd.DataFrame(stats, columns = columns)

# Convert into Spark DataFrame
df_stats = spark.createDataFrame(df)

# Fill empty strings with missing values
df_stats = df_stats.select([when(col(c)== "", None).otherwise(col(c)).alias(c) for c in df_stats.columns])

# Upload pyspark DataFrame as Parquet format into S3 Bucket
df_stats.write.parquet("s3a://msin0166nbadata/df_stats/")
