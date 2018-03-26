'''

http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&city=new+york&childtype=neighborhood
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&city=new+york&childtype=zipcode

http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&childtype=neighborhood
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&childtype=zipcode


upper west side 
median list price / square feet
https://www.zillow.com/market-report/time-series/270958/upper-west-side-new-york-ny.xls?m=35

rental list price / square feet
https://www.zillow.com/market-report/time-series/270958/upper-west-side-new-york-ny.xls?m=48

step 1:
scrape
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&childtype=neighborhood
to get all the neighborhood you can for new york state

step 2:
scrape
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&childtype=zipcode
to get all the zip code info you can for all the zip codes in the new york state and their heighborhood codes

step 3:
merge the two tables, and place them in MySQL database
'''

import numpy as np
import pandas as pd
import requests
from lxml import etree
from bs4 import BeautifulSoup

zillow_key = 'X1-ZWz1fwjz1gd6a3_8w9nb'
NY_neighborhood_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state=ny&childtype=neighborhood'
NY_zipcode_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state=ny&childtype=zipcode'

'''
NY_neighborhood = etree.parse(NY_neighborhood_link)

for neighborhood in NY_neighborhood.xpath('//subregiontype'):
    for ID in neighborhood.xpath('//id'):
        print(ID.text)
'''

region_content = requests.get(NY_zipcode_link).content
region_soup = BeautifulSoup(region_content, 'lxml')

RegionID_bs = region_soup.find_all('id')
zipcode_bs = region_soup.find_all('name')
lat_bs = region_soup.find_all('latitude')
long_bs = region_soup.find_all('longitude')

RegionID = [i.get_text() for i in RegionID_bs]
zipcode = [i.get_text() for i in zipcode_bs]
zipcode.insert(0, np.nan)
lat = [float(i.get_text()) for i in lat_bs]
long = [float(i.get_text()) for i in long_bs]

df_region = pd.DataFrame(np.array([RegionID, zipcode, lat, long]).T).rename(columns = {0:'RegionID', 1:'zip', 2:'lat', 3:'long'}).set_index('RegionID')

neighborhood_content = requests.get(NY_neighborhood_link).content
neighborhood_soup = BeautifulSoup(neighborhood_content, 'lxml')

#is this a complete list?? combine with the other list to see what's missing
#what's missing seems to be a small portion, and in the NY_zip file, there also seems to be duplicates
'''
NY_zipname = r'New_York_State_ZIP_Codes-County_FIPS_Cross-Reference.csv'
NY_zip = pd.read_csv(NY_zipname, usecols = ['County Name','ZIP Code'], dtype = {'ZIP Code':str})

temp = NY_zip.merge(df.reset_index(), how = 'left', left_on = 'ZIP Code', right_on = 'zip')
temp[(temp['zip'].isnull())].to_csv('missing_zips.csv')
'''

RegionID_bs = neighborhood_soup.find_all('id')
Name_bs = neighborhood_soup.find_all('name')
URL_bs = neighborhood_soup.find_all('url')
lat_bs = neighborhood_soup.find_all('latitude')
long_bs = neighborhood_soup.find_all('longitude')

RegionID = [i.get_text() for i in RegionID_bs]
Name = [i.get_text() for i in Name_bs]
Name.insert(0, np.nan)
URL = [i.get_text() for i in URL_bs]
URL.insert(0, np.nan)
lat = [float(i.get_text()) for i in lat_bs]
long = [float(i.get_text()) for i in long_bs]

df_neighborhood = pd.DataFrame(np.array([RegionID, Name, URL, lat, long]).T).rename(columns = {0:'RegionID', 1:'Name', 2:'URL', 3:'lat', 4:'long'}).set_index('RegionID')

#WTF for some reason the regions doesn't match
#df_region.merge(df_neighborhood, how = 'left', left_index = True, right_index = True)

#lets see what data this gets me
#https://www.zillow.com/market-report/time-series/270958/upper-west-side-new-york-ny.xls?m=35
#this will give you the neighborhood file
# I don't think I need this ('https://www.zillow.com/market-report/time-series/'+whatever[6][2:]+'/'+whatever[5]+'-'+whatever[4][3:]+'-'+whatever[4][:2]+'.xls?m=35').lower()

# I think I need the zipcode file
#home value files:
#zip code Home Prices & Values
#this is for 11238, select ZHVI, All homes, max
#name this zhvi_url
#"https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=zhvi_plus_forecast

#median list price, all homes, max
#name this median_list_url
#"https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=18

#median list price sq feet, all homes, max
#name this median_list_sqft_url
#https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=35
#https://www.zillow.com/market-report/time-series/399545/new-york-ny-10065.xls?m=35

#rental value files:
#zillow rental index
#name this zri_url
#https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=50
#https://www.zillow.com/market-report/time-series/399545/new-york-ny-10065.xls?m=50

#rent per square feet:
#name this rental_sqft_url
#https://www.zillow.com/market-report/time-series/399545/new-york-ny-10065.xls?m=48
content = pd.DataFrame()
xls = pd.ExcelFile(r'https://www.zillow.com/market-report/time-series/399545/new-york-ny-10065.xls?m=48')
for i in range(len(xls.sheet_names)):
    content = pd.concat([content, xls.parse(i, header=1)], axis = 0)

#fuck the neighborhoods

    
#bookmark
'''
plan B:
have state, scrape county, city, neighborhood, then zipcode
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&childtype=city&zipcode

for each city, get the zip codes within the city
http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&city=Amherst&childtype=zipcode
'''

zillow_key = 'X1-ZWz1fwjz1gd6a3_8w9nb'
state = 'ny'
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
counties = {}
df_county = pd.DataFrame(np.array([countyID,
                                   countyName,
                                   countyLat,
                                   countyLong]).T).rename(columns = {0:'countyID',
                                                                     1:'countyName',
                                                                     2:'countyLat',
                                                                     3:'countyLong'})


counties[state] = df_county
# bookmark

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
            

            cities[(state, countyName)] = df_city

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
        neighbors[(state, countyName, city)] = df_neighbor


zipcodes = {}




city_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state='+state+'&childtype=city'

city_content = requests.get(city_link).content
city_soup = BeautifulSoup(city_content, 'lxml')

cityID_bs = city_soup.find_all('id')
cityName_bs = city_soup.find_all('name')
cityURL_bs = city_soup.find_all('url')
cityLat_bs = city_soup.find_all('latitude')
cityLong_bs = city_soup.find_all('longitude')

cityID = [i.get_text() for i in cityID_bs][1:]
cityName = [i.get_text() for i in cityName_bs]
cityURL = [i.get_text() for i in cityURL_bs]
cityLat = [float(i.get_text()) for i in cityLat_bs][1:]
cityLong = [float(i.get_text()) for i in cityLong_bs][1:]

#running into trouble, there are two of the same name cities.

df_city = pd.DataFrame(np.array([cityID, cityName, cityURL, cityLat, cityLong]).T).rename(columns = {0:'cityID', 1:'cityName', 2:'cityURL', 3:'cityLat', 4:'cityLong'})

for city in df_city.cityName.unique()[:5]:
    neighborhood_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state='+state+'&city='+city+'&childtype=neighborhood'
    
    

NY_zipcode_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key+'&state=ny&childtype=zipcode'



