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
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

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
    
    
# function to get url link for each team's roster
def find_team_url():
    url = "http://www.espn.com/nba/teams"
    r = requests.get(url, headers = headers)
    text = r.text
    soup = BeautifulSoup(text)
    
    # Find all 'div' tags where roster can be located
    div_tags = soup.find_all('div', class_= "TeamLinks__Links")
    
    # Get link for each team's roster
    links = []
    for div in div_tags:
        span = div.select('span')[2]
        links.append(span.a.get('href'))
        
    team_rosters = {}
    for link in links:
        team_name = link.rsplit('/', 1)[-1]
        team_rosters[team_name] = 'http://www.espn.com' + link
    return team_rosters
    
    
rosters = find_team_url()



def get_player_data():
    d = {}
    for k, v in rosters.items():
        d[k] = pd.DataFrame()
        r = requests.get(v, headers = headers)
        text = r.text
        soup = BeautifulSoup(text)
        
        # Identify table on webpage
        table = soup.find('table', attrs = {'class': 'Table'})
        
        # Obtain column headers
        columns = []
        for i in table.find_all('th'):
            title = i.text
            columns.append(title)
        
        d[k] = pd.DataFrame(columns = columns)
    
        # Create a for loop to fill dataframe
        for j in table.find_all('tr')[1:]:
            row_data = j.find_all('td')
            row = [i.text for i in row_data]
            length = len(d[k])
            d[k].loc[length] = row

        d[k]['team'] = k # Add team name for the players
        time.sleep(random.randint(2,5)) # Space out each request so server isn't overwhelmed
    return d
    
    
    
player_data = get_player_data()


# Join dataframes for each team roster
full_teams = pd.concat(player_data.values(), ignore_index=True)
full_teams
    
# Convert into Spark DataFrame
df_rosters = spark.createDataFrame(full_teams)



# Clean jersey number from player names
df_rosters = df_rosters.withColumn("Name", regexp_replace('Name', "[0-9]", ""))

# Clean height column to convert to inches
df_rosters = df_rosters.withColumn("HT", regexp_replace('HT', '[\s\'"]', ""))
df_rosters = df_rosters.withColumn("HT", df_rosters.HT.substr(1,1) * 12 + df_rosters.HT.substr(2,2)) 

# Clean weight column to remove lbs
df_rosters = df_rosters.withColumn("WT", regexp_replace('WT', "[^0-9]", ""))

# Remove hypens from missing values in College
df_rosters = df_rosters.withColumn("College", regexp_replace('College', "[^a-zA-Z]", ""))

# Clean salary to only include number
df_rosters = df_rosters.withColumn("Salary", regexp_replace('Salary', "[^0-9]", ""))

# Drop empty first column
df_rosters = df_rosters.drop('')

# Fill empty strings with missing values
df_rosters = df_rosters.select([when(col(c)== "", None).otherwise(col(c)).alias(c) for c in df_rosters.columns])


# Upload pyspark DataFrame as Parquet format into S3 Bucket
df_rosters.write.parquet("s3a://msin0166nbadata/df_rosters/")
    
    
