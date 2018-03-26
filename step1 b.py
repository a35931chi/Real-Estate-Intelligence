import numpy as np
import pandas as pd
import requests
from lxml import etree
from bs4 import BeautifulSoup


zillow_key = 'X1-ZWz1fwjz1gd6a3_8w9nb'
state = 'ny' # we should have a list of states, for now, we'll work with new york state

counties = {}
county_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state='+state+'&childtype=county'

county_content = requests.get(county_link).content
county_soup = BeautifulSoup(county_content, 'lxml')

countyID_bs = county_soup.find_all('id')
countyName_bs = county_soup.find_all('name')
countyLat_bs = county_soup.find_all('latitude')
countyLong_bs = county_soup.find_all('longitude')

countyID = [i.get_text() for i in countyID_bs][1:]
countyName = [i.get_text().lower().replace(' ', '+') for i in countyName_bs]
countyLat = [float(i.get_text()) for i in countyLat_bs][1:]
countyLong = [float(i.get_text()) for i in countyLong_bs][1:]

df_county = pd.DataFrame(np.array([countyID,
                                   countyName,
                                   countyLat,
                                   countyLong]).T).rename(columns = {0:'countyID',
                                                                     1:'countyName',
                                                                     2:'countyLat',
                                                                     3:'countyLong'})

counties[state] = df_county

cities = {}
for state, content in counties.items():
    
    for county in content[:5]['countyName']:
        
        city_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state='+state+'&county='+county+'&childtype=city'
        city_content = requests.get(city_link).content
        city_soup = BeautifulSoup(city_content, 'lxml')
        cityID_bs = city_soup.find_all('id')
        cityName_bs = city_soup.find_all('name')
        cityURL_bs = city_soup.find_all('url')
        cityLat_bs = city_soup.find_all('latitude')
        cityLong_bs = city_soup.find_all('longitude')
        cityID = [i.get_text() for i in cityID_bs][1:]
        cityName = [i.get_text().lower().replace(' ','+') for i in cityName_bs]
        cityURL = [i.get_text() for i in cityURL_bs]
        cityLat = [float(i.get_text()) for i in cityLat_bs][1:]
        cityLong = [float(i.get_text()) for i in cityLong_bs][1:]
        df_city = pd.DataFrame(np.array([cityID,
                                         cityName,
                                         cityURL,
                                         cityLat,
                                         cityLong]).T).rename(columns = {0:'cityID',
                                                                         1:'cityName',
                                                                         2:'cityURL',
                                                                         3:'cityLat',
                                                                         4:'cityLong'})
            

        cities[(state, county)] = df_city

neighbors = {}    
for statecounty, content in cities.items():
    state, county = statecounty
    for city in content[:5]['cityName']:
        neighbor_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state='+state+'&county='+county+'&city='+city+'&childtype=neighborhood'
        neighbor_content = requests.get(neighbor_link).content
        neighbor_soup = BeautifulSoup(neighbor_content, 'lxml')
                
        neighborID_bs = neighbor_soup.find_all('id')
        neighborName_bs = neighbor_soup.find_all('name')
        neighborLat_bs = neighbor_soup.find_all('latitude')
        neighborLong_bs = neighbor_soup.find_all('longitude')

        neighborID = [i.get_text() for i in cityID_bs][1:]
        neighborName = [i.get_text().lower().replace(' ','+') for i in cityName_bs]
                
        neighborLat = [float(i.get_text()) for i in cityLat_bs][1:]
        neighborLong = [float(i.get_text()) for i in cityLong_bs][1:]
        df_neighbor = pd.DataFrame(np.array([neighborID,
                                             neighborName,
                                             neighborLat,
                                             neighborLong]).T).rename(columns = {0:'neighborID',
                                                                                 1:'neighborName',
                                                                                 2:'neighborLat',
                                                                                 3:'neighborLong'})
        neighbors[(state, county, city)] = df_neighbor

