import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from random import randint


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