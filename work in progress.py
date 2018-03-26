import numpy as np
import pandas as pd
import requests
from lxml import etree
from bs4 import BeautifulSoup
import time
import datetime
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine, MetaData, String, Integer, Table, Column, ForeignKey
import quandl
from itertools import cycle
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats.stats import pearsonr
import scipy.stats as stats

'''
new strategy
1. find all zip codes in ny state

2. find all counties in ny state
2a. find all zip codes in those counties

3. find all cities in ny state
3a. find all zip codes in those cities

4. find all neighborhoods in ny state
4a. find all zip codes in those neighborhoods

'''

def zillow_zipcode_pull():
    zillow_key = ['X1-ZWz1fwjz1gd6a3_8w9nb',
                  'X1-ZWz1fxenyrfwgb_aroxz',
                  'X1-ZWz1933l3jqt57_aypsc']

    state = 'ny' # we should have a list of states, for now, we'll work with new york state



    #step 1: get all the zip codes in the state. if we have a list of states, we need to loop through the states
    state_zip_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[2]+'&state='+state+'&childtype=zipcode'

    state_zip_content = requests.get(state_zip_link).content
    state_zip_soup = BeautifulSoup(state_zip_content, 'lxml')

    approval = state_zip_soup.find_all('code')

    if approval[0].get_text() == '0':
        state_zip_ID_bs = state_zip_soup.find_all('id')
        state_zip_Name_bs = state_zip_soup.find_all('name')
        state_zip_Lat_bs = state_zip_soup.find_all('latitude')
        state_zip_Long_bs = state_zip_soup.find_all('longitude')

        state_zip_ID = [i.get_text() for i in state_zip_ID_bs][1:]
        state_zip_Name = [i.get_text().lower().replace(' ', '+') for i in state_zip_Name_bs]
        state_zip_Lat = [float(i.get_text()) for i in state_zip_Lat_bs][1:]
        state_zip_Long = [float(i.get_text()) for i in state_zip_Long_bs][1:]

        df_state_zip = pd.DataFrame(np.array([state_zip_ID,
                                              state_zip_Name,
                                              state_zip_Lat,
                                              state_zip_Long]).T).rename(columns = {0:'state_zip_ID',
                                                                                    1:'state_zip_Name',
                                                                                    2:'state_zip_Lat',
                                                                                    3:'state_zip_Long'})

    '''
    #step 2: get all the counties in the state
    state_county_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[0]+'&state='+state+'&childtype=county'

    state_county_content = requests.get(state_county_link).content
    state_county_soup = BeautifulSoup(state_county_content, 'lxml')

    approval = state_county_soup.find_all('code')

    if approval[0].get_text() == '0':
        state_county_ID_bs = state_county_soup.find_all('id')
        state_county_Name_bs = state_county_soup.find_all('name')
        state_county_URL_bs = state_county_soup.find_all('url')
        state_county_Lat_bs = state_county_soup.find_all('latitude')
        state_county_Long_bs = state_county_soup.find_all('longitude')

        state_county_ID = [i.get_text() for i in state_county_ID_bs][1:]
        state_county_Name = [i.get_text().lower().replace(' ', '+') for i in state_county_Name_bs]
        state_county_Lat = [float(i.get_text()) for i in state_county_Lat_bs][1:]
        state_county_Long = [float(i.get_text()) for i in state_county_Long_bs][1:]

        df_state_county = pd.DataFrame(np.array([state_county_ID,
                                                 state_county_Name,
                                                 state_county_Lat,
                                                 state_county_Long]).T).rename(columns = {0:'state_county_ID',
                                                                                          1:'state_county_Name',
                                                                                          2:'state_county_Lat',
                                                                                          3:'state_county_Long'})
        df_state_county['state'] = state

    #step 2a: get all the zipcodes in the counties in the state

    df_state_county_zip_combined = pd.DataFrame()
    for state in df_state_county['state'].unique():
        for county in df_state_county[df_state_county['state'] == state]['state_county_Name'].unique():
            state_county_zip_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[0]+'&state='+state+'county='+county+'&childtype=zipcode'

            state_county_zip_content = requests.get(state_county_zip_link).content
            state_county_zip_soup = BeautifulSoup(state_county_zip_content, 'lxml')

            approval = state_county_zip_soup.find_all('code')

            if approval[0].get_text() == '0':
                state_county_zip_ID_bs = state_county_zip_soup.find_all('id')
                state_county_zip_Name_bs = state_county_zip_soup.find_all('name')
                state_county_zip_Lat_bs = state_county_zip_soup.find_all('latitude')
                state_county_zip_Long_bs = state_county_zip_soup.find_all('longitude')

                state_county_zip_ID = [i.get_text() for i in state_county_zip_ID_bs][1:]
                state_county_zip_Name = [i.get_text().lower().replace(' ', '+') for i in state_county_zip_Name_bs]
                state_county_zip_Lat = [float(i.get_text()) for i in state_county_zip_Lat_bs][1:]
                state_county_zip_Long = [float(i.get_text()) for i in state_county_zip_Long_bs][1:]

                df_state_county_zip = pd.DataFrame(np.array([state_county_zip_ID,
                                                             state_county_zip_Name,
                                                             state_county_zip_Lat,
                                                             state_county_zip_Long]).T).rename(columns = {0:'state_county_zip_ID',
                                                                                                          1:'state_county_zip_Name',
                                                                                                          2:'state_county_zip_Lat',
                                                                                                          3:'state_county_zip_Long'})
                df_state_county_zip['state'] = state
                df_state_county_zip['county'] = county
                df_state_county_zip_combined = pd.concat([df_state_county_zip_combined, df_state_county_zip], axis = 0)
    '''
    #Step 3: find all cities in the state
    state_city_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[2]+'&state='+state+'&childtype=city'

    state_city_content = requests.get(state_city_link).content
    state_city_soup = BeautifulSoup(state_city_content, 'lxml')

    approval = state_city_soup.find_all('code')

    if approval[0].get_text() == '0':
        state_city_ID_bs = state_city_soup.find_all('id')
        state_city_Name_bs = state_city_soup.find_all('name')
        state_city_URL_bs = state_city_soup.find_all('url')
        state_city_Lat_bs = state_city_soup.find_all('latitude')
        state_city_Long_bs = state_city_soup.find_all('longitude')

        state_city_ID = [i.get_text() for i in state_city_ID_bs][1:]
        state_city_Name = [i.get_text().lower().replace(' ', '+') for i in state_city_Name_bs]
        state_city_Lat = [float(i.get_text()) for i in state_city_Lat_bs][1:]
        state_city_Long = [float(i.get_text()) for i in state_city_Long_bs][1:]

        df_state_city = pd.DataFrame(np.array([state_city_ID,
                                               state_city_Name,
                                               state_city_Lat,
                                               state_city_Long]).T).rename(columns = {0:'state_city_ID',
                                                                                      1:'state_city_Name',
                                                                                      2:'state_city_Lat',
                                                                                      3:'state_city_Long'})
        df_state_city['state'] = state


    #Step 3a: find all zip codes in those cities
    df_state_city_zip_combined = pd.DataFrame()
    for state in df_state_city['state'].unique():
        for city in df_state_city[df_state_city['state'] == state]['state_city_Name'].unique():
            print(city)
            state_city_zip_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[2]+'&state='+state+'&city='+city+'&childtype=zipcode'

            state_city_zip_content = requests.get(state_city_zip_link).content
            state_city_zip_soup = BeautifulSoup(state_city_zip_content, 'lxml')

            approval = state_city_zip_soup.find_all('code')

            if approval[0].get_text() == '0':
                state_city_zip_ID_bs = state_city_zip_soup.find_all('id')
                state_city_zip_Name_bs = state_city_zip_soup.find_all('name')
                state_city_zip_Lat_bs = state_city_zip_soup.find_all('latitude')
                state_city_zip_Long_bs = state_city_zip_soup.find_all('longitude')

                state_city_zip_ID = [i.get_text() for i in state_city_zip_ID_bs][1:]
                state_city_zip_Name = [i.get_text().lower().replace(' ', '+') for i in state_city_zip_Name_bs]
                state_city_zip_Lat = [float(i.get_text()) for i in state_city_zip_Lat_bs][1:]
                state_city_zip_Long = [float(i.get_text()) for i in state_city_zip_Long_bs][1:]

                df_state_city_zip = pd.DataFrame(np.array([state_city_zip_ID,
                                                           state_city_zip_Name,
                                                           state_city_zip_Lat,
                                                           state_city_zip_Long]).T).rename(columns = {0:'state_city_zip_ID',
                                                                                                      1:'state_city_zip_Name',
                                                                                                      2:'state_city_zip_Lat',
                                                                                                      3:'state_city_zip_Long'})
                df_state_city_zip['state'] = state
                df_state_city_zip['city'] = city
                df_state_city_zip_combined = pd.concat([df_state_city_zip_combined, df_state_city_zip], axis = 0)
            time.sleep(0.75)

    '''
    #Step 4: find all neighborhoods in the state
    state_neighbor_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[1]+'&state='+state+'&childtype=neighborhood'

    state_neighbor_content = requests.get(state_neighbor_link).content
    state_neighbor_soup = BeautifulSoup(state_neighbor_content, 'lxml')

    approval = state_neighbor_soup.find_all('code')

    if approval[0].get_text() == '0':
        state_neighbor_ID_bs = state_neighbor_soup.find_all('id')
        state_neighbor_Name_bs = state_neighbor_soup.find_all('name')
        state_neighbor_URL_bs = state_neighbor_soup.find_all('url')
        state_neighbor_Lat_bs = state_neighbor_soup.find_all('latitude')
        state_neighbor_Long_bs = state_neighbor_soup.find_all('longitude')

        state_neighbor_ID = [i.get_text() for i in state_neighbor_ID_bs][1:]
        state_neighbor_Name = [i.get_text().lower().replace(' ', '+') for i in state_neighbor_Name_bs]
        state_neighbor_Lat = [float(i.get_text()) for i in state_neighbor_Lat_bs][1:]
        state_neighbor_Long = [float(i.get_text()) for i in state_neighbor_Long_bs][1:]

        df_state_neighbor = pd.DataFrame(np.array([state_neighbor_ID,
                                               state_neighbor_Name,
                                               state_neighbor_Lat,
                                               state_neighbor_Long]).T).rename(columns = {0:'state_neighbor_ID',
                                                                                      1:'state_neighbor_Name',
                                                                                      2:'state_neighbor_Lat',
                                                                                      3:'state_neighbor_Long'})
        df_state_neighbor['state'] = state
            

    '''

    '''
    #put things into sql table
    import pymysql
    import pymysql.cursors
    from sqlalchemy import create_engine, MetaData, String, Integer, Table, Column, ForeignKey

    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    df_state_city_zip_combined.to_sql('state_city_zip', engine, if_exists = 'replace', index = True)
    df_state_city.to_sql('state_city', engine, if_exists = 'replace', index = True)

    df_state_zip.to_sql('state_zip', engine, if_exists = 'replace', index = True)
    '''

    df_state_city_zip_combined.shape #(1690, 6)
    df_state_city.shape #(999, 5)
    df_state_zip.shape #(999, 4)


#using pandas to read things using data in df_state_city_zip_combined.

#reference document
#this is for 11238, select ZHVI, All homes, max
#name this zhvi_url
#https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=zhvi_plus_forecast

#median list price, all homes, max
#name this median_list_url
#https://www.zillow.com/market-report/time-series/62048/new-york-ny-11238.xls?m=18

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



def zillow_init():
    #start here
    #if disconnected, load from MySQL
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    df_state_city_zip_combined = pd.read_sql('select * from state_city_zip;', engine)

    df_state_city_zip_combined['zhvi_url'] = r'https://www.zillow.com/market-report/time-series/'+df_state_city_zip_combined['state_city_zip_ID']+'/'+df_state_city_zip_combined['city']+'-'+df_state_city_zip_combined['state_city_zip_Name']+'.xls?m=zhvi_plus_forecast'
    df_state_city_zip_combined['med_list_url'] = r'https://www.zillow.com/market-report/time-series/'+df_state_city_zip_combined['state_city_zip_ID']+'/'+df_state_city_zip_combined['city']+'-'+df_state_city_zip_combined['state']+'-'+df_state_city_zip_combined['state_city_zip_Name']+'.xls?m=18'
    df_state_city_zip_combined['med_list_sqft_url'] = r'https://www.zillow.com/market-report/time-series/'+df_state_city_zip_combined['state_city_zip_ID']+'/'+df_state_city_zip_combined['city']+'-'+df_state_city_zip_combined['state']+'-'+df_state_city_zip_combined['state_city_zip_Name']+'.xls?m=35'
    df_state_city_zip_combined['zri_url'] = r'https://www.zillow.com/market-report/time-series/'+df_state_city_zip_combined['state_city_zip_ID']+'/'+df_state_city_zip_combined['city']+'-'+df_state_city_zip_combined['state']+'-'+df_state_city_zip_combined['state_city_zip_Name']+'.xls?m=50'
    df_state_city_zip_combined['rental_sqft_url'] = r'https://www.zillow.com/market-report/time-series/'+df_state_city_zip_combined['state_city_zip_ID']+'/'+df_state_city_zip_combined['city']+'-'+df_state_city_zip_combined['state']+'-'+df_state_city_zip_combined['state_city_zip_Name']+'.xls?m=48'

    print(df_state_city_zip_combined.shape) #(1690, 12)
    return df_state_city_zip_combined



def quandl_init(key):
    Quandlkey = ['7ieY8tq_kjzWx2-DiyGx', 'XrVxmKtfg2Fo3FG_NmtC', 'Jh3CAbmwaNP7YoqAN4FK']
    if key == None:
        quandl.ApiConfig.api_key = Quandlkey[0]
        return Quandlkey[0]
    else:
        if Quandlkey.index(key) == 2:
            quandl.ApiConfig.api_key = Quandlkey[0]
            return Quandlkey[0]
        else:
            quandl.ApiConfig.api_key = Quandlkey[Quandlkey.index(key) + 1]
            return Quandlkey[Quandlkey.index(key) + 1]



def zhvi(df_state_city_zip_combined):
    #zillow home price index portion
    zhvi = pd.DataFrame()

    for link in df_state_city_zip_combined['zhvi_url']:
        print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                zhvi = pd.concat([zhvi, temp], axis = 0)
        except Exception as e:
            print(e)

    print(zhvi.shape) #(99408, 123)
    print(zhvi.duplicated(keep = 'first').sum()) #93587
    zhvi.drop_duplicates(keep = 'first', inplace = True) 
    print(zhvi.shape) #(5821, 123)
    print(zhvi.duplicated(keep = 'first').sum())

    #transpose this dataframe
    zhvi_T = zhvi.set_index(['Data Type','Region Name','Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'zhvi'})
    zhvi_T['Region Name'] = zhvi_T['Region Name'].apply(lambda x: str(x).zfill(5))
    #we need to drop some columns and rename some columns
    zhvi_T.rename(columns = {'Data Type':'Home Type', 'Region Name': 'zip'}, inplace = True)
    zhvi_T.drop(['Region Type'], inplace = True, axis = 1)
    print(zhvi_T.shape) #679277, 4)
    zhvi_T['d'] = 2

    #standard code to pull my data
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')

    #zillow home price index portion
    oldz = pd.read_sql('select * from zillow_zhvi;', engine)
    oldz['d'] = 4
    print(oldz.shape) #(623007, 4)

    #stack the zillow tables
    temp = pd.concat([oldz, zhvi_T], axis = 0) #(1302284, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #616946

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(1233892, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(15264, 4)
    dups['p_diff'] = (dups.xs('zhvi', level=0, axis=1).max(axis = 1) - dups.xs('zhvi', level=0, axis=1).min(axis = 1)) / dups.xs('zhvi', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 3%, which is kinda annoying but acceptable
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(685338, 5)
    tempa.drop('d', axis = 1).to_sql('zillow_zhvi', engine, if_exists = 'replace', index = False) 
     
 
    quandl_zhvi = {'ZHVIAH': 'All Homes',
                   'ZHVISAH': 'All Homes',
                   'ZHVICO': 'Condo',
                   'ZHVI1B': 'One Bed',
                   'ZHVI2B': 'Two Bed',
                   'ZHVI3B': 'Three Bed',
                   'ZHVI4B': 'Four Bed',
                   'ZHVI5B': 'Many Bed',
                   'ZHVISF': 'Single Fam',
                   'ZHVITT': 'Top Tier',
                   'ZHVIMT': 'Middle Tier',
                   'ZHVIBT': 'Bottom Tier'}
    # quandl zhvi
    df_quandl_zhvi = pd.DataFrame()

    current_key = quandl_init(None)

    for zip in df_state_city_zip_combined['state_city_zip_Name']:
        for key in quandl_zhvi.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_zhvi[key]
                a.rename(columns = {'Value': 'zhvi'}, inplace = True)
                df_quandl_zhvi = pd.concat([df_quandl_zhvi, a], axis = 0)
            except Exception as e:
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_zhvi[key]
                        a.rename(columns = {'Value': 'zhvi'}, inplace = True)
                        df_quandl_zhvi = pd.concat([df_quandl_zhvi, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))
  
    print(df_quandl_zhvi.shape) #(1321111, 3)
    print(df_quandl_zhvi.duplicated(keep = 'first').sum()) #231516
    df_quandl_zhvi.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_zhvi.shape) #(1089595, 3)
    print(df_quandl_zhvi.duplicated(keep = 'first').sum())
    df_quandl_zhvi.index.name = 'Date'
    df_quandl_zhvi.reset_index(inplace = True)
    df_quandl_zhvi['d'] = 1

    #quandl home price index portion
    oldq = pd.read_sql('select * from quandl_zhvi;', engine)
    oldq['d'] = 3
    print(oldq.shape) #(1061262, 4)

    temp = pd.concat([oldq, df_quandl_zhvi], axis = 0) #(2150857, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #1019512

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(2039024, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(1019512, 5)
    dups['p_diff'] = (dups.xs('zhvi', level=0, axis=1).max(axis = 1) - dups.xs('zhvi', level=0, axis=1).min(axis = 1)) / dups.xs('zhvi', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying. two observations
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(1131345, 5)
    tempa.drop('d', axis = 1).to_sql('quandl_zhvi', engine, if_exists = 'replace', index = False) 

    #datasource comparision
    #zillow home value index (ZHVI)
    #do some work to compare the two database
    print(oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].shape) #(139, 4)
    print(zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].shape) #(157, 4)

    oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].to_csv('oz.csv', index = False)
    oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].to_csv('oq.csv', index = False)
    zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #combine these three tables: 1. oldz oldq, 2. zillow, 3. quandl
    #stack the four tables
    temp = pd.concat([oldz, oldq, zhvi_T, df_quandl_zhvi], axis = 0) #(3453141, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #2190505

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(3424875, 5)
    #dups['ab'] = dups[['Date', 'Home Type', 'zip']].duplicated(keep = 'first') 
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(15264, 4)
    dups['p_diff'] = (dups.xs('zhvi', level=0, axis=1).max(axis = 1) - dups.xs('zhvi', level=0, axis=1).min(axis = 1)) / dups.xs('zhvi', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))].drop('d', axis = 1) #(1262636, 4)


    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(1354255, 4)

    temp1.to_sql('combined_zhvi', engine, if_exists = 'replace', index = True)

       

def median_list(df_state_city_zip_combined):
    #zillow median list portion
    med_list = pd.DataFrame()

    for link in df_state_city_zip_combined['med_list_url']:
        print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                med_list = pd.concat([med_list, temp], axis = 0) 
        except Exception as e:
            print(e)

    print(med_list.shape) #(47637, 94)
    print(med_list.duplicated(keep = 'first').sum()) #46166
    med_list.drop_duplicates(keep = 'first', inplace = True)
    print(med_list.shape) #(1471, 94)
    print(med_list.duplicated(keep = 'first').sum())

    med_list_T = med_list.set_index(['Data Type','Region Name','Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'med_list'})
    med_list_T['Region Name'] = med_list_T['Region Name'].apply(lambda x: str(x).zfill(5))
    med_list_T.rename(columns = {'Data Type':'Home Type', 'Region Name': 'zip'}, inplace = True)
    med_list_T.drop(['Region Type'], inplace = True, axis = 1)
    med_list_T['d'] = 2

    #standard code to pull my data
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')

    #zillow home price index portion
    oldz = pd.read_sql('select * from zillow_med_list;', engine)
    oldz['d'] = 4
    print(oldz.shape) #(84254, 5)

    #stack the zillow tables
    temp = pd.concat([oldz, med_list_T], axis = 0) #(169342, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #82809

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(1233892, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(15264, 4)
    dups['p_diff'] = (dups.xs('med_list', level=0, axis=1).max(axis = 1) - dups.xs('med_list', level=0, axis=1).min(axis = 1)) / dups.xs('med_list', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 3%, which is kinda annoying but acceptable
    print(dups.sort_values(by = 'p_diff', ascending = False).head())
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(86533, 5)
    tempa.drop('d', axis = 1).to_sql('zillow_med_list', engine, if_exists = 'replace', index = False) 


    #quandl medium listing
    quandl_med_list = {'MLPAH': 'All Homes',
                       'MLPAH': 'All Homes',
                       'MLPCC': 'Condo',
                       'MLPDT': 'Duplex/Triplex',
                       'MLP1B': 'One Bed',
                       'MLP2B': 'Two Bed',
                       'MLP3B': 'Three Bed',
                       'MLP4B': 'Four Bed',
                       'MLP5B': 'Many Bed',
                       'MLPSF': 'Single Fam'}

    df_quandl_med_list = pd.DataFrame()

    for zip in df_state_city_zip_combined['state_city_zip_Name']:
        for key in quandl_med_list.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_med_list[key]
                a.rename(columns = {'Value': 'med_list'}, inplace = True)
                df_quandl_med_list = pd.concat([df_quandl_med_list, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_med_list[key]
                        a.rename(columns = {'Value': 'med_list'}, inplace = True)
                        df_quandl_med_list = pd.concat([df_quandl_med_list, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_med_list.shape) #(97806, 3)
    print(df_quandl_med_list.duplicated(keep = 'first').sum()) #36351
    df_quandl_med_list.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_list.shape) #(61455, 3)
    print(df_quandl_med_list.duplicated(keep = 'first').sum())
    df_quandl_med_list.index.name = 'Date'
    df_quandl_med_list.reset_index(inplace = True)
    df_quandl_med_list['d'] = 1

    #quandl home price index portion
    oldq = pd.read_sql('select * from quandl_med_list;', engine)
    oldq['d'] = 3
    print(oldq.shape) #(60279, 5)

    temp = pd.concat([oldq, df_quandl_med_list], axis = 0) #(121734, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #59176

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(61455, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(1019512, 5)
    dups['p_diff'] = (dups.xs('med_list', level=0, axis=1).max(axis = 1) - dups.xs('med_list', level=0, axis=1).min(axis = 1)) / dups.xs('med_list', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 8%, which is kinda annoying. 3 observations
    print(dups.sort_values(by = 'p_diff', ascending = False).head(10))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(62558, 5)
    tempa.drop('d', axis = 1).to_sql('quandl_med_list', engine, if_exists = 'replace', index = False)

    #datasource comparision
    #zillow home value index (ZHVI), we haven't changed this
    #do some work to compare the two database
    print(oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].shape) #(139, 4)
    print(zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].shape) #(157, 4)

    oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].to_csv('oz.csv', index = False)
    oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].to_csv('oq.csv', index = False)
    zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #combine these three tables: 1. oldz oldq, 2. zillow, 3. quandl
    #stack the four tables
    temp = pd.concat([oldz, oldq, med_list_T, df_quandl_med_list], axis = 0) #(291076, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #197389

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(289525, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(15264, 4)
    dups['p_diff'] = (dups.xs('med_list', level=0, axis=1).max(axis = 1) - dups.xs('med_list', level=0, axis=1).min(axis = 1)) / dups.xs('med_list', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying
    print(dups.sort_values(by = 'p_diff', ascending = False).head(10))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))].drop('d', axis = 1) #(93687, 4)


    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(97730, 4)

    temp1.to_sql('combined_med_list', engine, if_exists = 'replace', index = True)


def zri(df_state_city_zip_combined):
    #zillow rental index portion
    zri = pd.DataFrame()

    for link in df_state_city_zip_combined['zri_url']:
        print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                zri = pd.concat([zri, temp], axis = 0) 
        except Exception as e:
            print(e)

    print(zri.shape) #(60850, 84)
    print(zri.duplicated(keep = 'first').sum()) #57887
    zri.drop_duplicates(keep = 'first', inplace = True)
    print(zri.shape) #(2963, 84)
    print(zri.duplicated(keep = 'first').sum())

    zri_T = zri.set_index(['Data Type','Region Name','Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'zri'})
    zri_T['Region Name'] = zri_T['Region Name'].apply(lambda x: str(x).zfill(5))
    zri_T.rename(columns = {'Data Type':'Home Type', 'Region Name': 'zip'}, inplace = True)
    zri_T.drop(['Region Type'], inplace = True, axis = 1)
    zri_T['d'] = 2

    #standard code to pull my data
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')

    #zillow home price index portion
    oldz = pd.read_sql('select * from zri;', engine) #remember to change
    oldz.rename(columns = {'zrvi': 'zri'}, inplace = True)
    oldz['d'] = 4
    print(oldz.shape) #(218352, 5)

    #stack the zillow tables
    temp = pd.concat([oldz, zri_T], axis = 0) #(439747, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #218352

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(436704, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(218352, 5)
    dups['p_diff'] = (dups.xs('zri', level=0, axis=1).max(axis = 1) - dups.xs('zri', level=0, axis=1).min(axis = 1)) / dups.xs('zri', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 3%, which is kinda annoying but acceptable
    print(dups.sort_values(by = 'p_diff', ascending = False).head())
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(221395, 5)
    tempa.drop('d', axis = 1).to_sql('zillow_zri', engine, if_exists = 'replace', index = False) 

    #Zillow Rental Index -
    quandl_zri = {'ZRIAHMF': 'All Homes Multi Fam',
                  'ZRIAH': 'All Homes',
                  'ZRIMFRR': 'Multi Fam',
                  'ZRISFRR': 'Single Fam'}

    df_quandl_zri = pd.DataFrame()

    for zip in df_state_city_zip_combined['state_city_zip_Name']:
        for key in quandl_zri.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_zri[key]
                a.rename(columns = {'Value': 'zri'}, inplace = True)
                df_quandl_zri = pd.concat([df_quandl_zri, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_zri[key]
                        a.rename(columns = {'Value': 'zri'}, inplace = True)
                        df_quandl_zri = pd.concat([df_quandl_zri, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_zri.shape) #(204738, 3)
    print(df_quandl_zri.duplicated(keep = 'first').sum()) #24120
    df_quandl_zri.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_zri.shape) #(180618, 3)
    print(df_quandl_zri.duplicated(keep = 'first').sum())
    df_quandl_zri.index.name = 'Date'
    df_quandl_zri.reset_index(inplace = True)
    df_quandl_zri['d'] = 1

    #quandl home price index portion
    oldq = pd.read_sql('select * from quandl_zri;', engine)
    oldq['d'] = 3
    print(oldq.shape) #(178522, 5)

    temp = pd.concat([oldq, df_quandl_zri], axis = 0) #(359140, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #168974

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(337948, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(168974, 5)
    dups['p_diff'] = (dups.xs('zri', level=0, axis=1).max(axis = 1) - dups.xs('zri', level=0, axis=1).min(axis = 1)) / dups.xs('zri', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 8%, which is kinda annoying. 3 observations
    print(dups.sort_values(by = 'p_diff', ascending = False).head(10))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(62558, 5)
    tempa.drop('d', axis = 1).to_sql('quandl_zri', engine, if_exists = 'replace', index = False)

    #datasource comparision
    #zillow home value index (ZHVI), we haven't changed this
    #do some work to compare the two database
    print(oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].shape) #(139, 4)
    print(zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].shape) #(157, 4)

    oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].to_csv('oz.csv', index = False)
    oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].to_csv('oq.csv', index = False)
    zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #combine these three tables: 1. oldz oldq, 2. zillow, 3. quandl
    #stack the four tables
    temp = pd.concat([oldz, oldq, zri_T, df_quandl_zri], axis = 0) #(798887, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #439385

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(781041, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(341656, 7)
    dups['p_diff'] = (dups.xs('zri', level=0, axis=1).max(axis = 1) - dups.xs('zri', level=0, axis=1).min(axis = 1)) / dups.xs('zri', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying
    print(dups.sort_values(by = 'p_diff', ascending = False).head(10))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))].drop('d', axis = 1) #(359502, 4)


    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(369643, 4)

    temp1.to_sql('combined_zri', engine, if_exists = 'replace', index = True)


def med_list_per_sqft(df_state_city_zip_combined):
    #zillow median list per sqft portion
    med_list_sqft = pd.DataFrame()

    for link in df_state_city_zip_combined['med_list_sqft_url']:
        print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                med_list_sqft = pd.concat([med_list_sqft, temp], axis = 0) 
        except Exception as e:
            print(e)

    print(med_list_sqft.shape) #(40025, 94)
    print(med_list_sqft.duplicated(keep = 'first').sum()) #38638
    med_list_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(med_list_sqft.shape) #(1387, 94)
    print(med_list_sqft.duplicated(keep = 'first').sum())

    med_list_sqft_T = med_list_sqft.set_index(['Data Type','Region Name','Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'med_list_sqft'})
    med_list_sqft_T['Region Name'] = med_list_sqft_T['Region Name'].apply(lambda x: str(x).zfill(5))
    med_list_sqft_T.rename(columns = {'Data Type':'Home Type', 'Region Name': 'zip'}, inplace = True)
    med_list_sqft_T.drop(['Region Type'], inplace = True, axis = 1)

    med_list_sqft_T['d'] = 2

    #standard code to pull my data
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')

    #zillow home price index portion
    oldz = pd.read_sql('select * from zillow_med_list_sqft;', engine) #remember to change
    oldz['d'] = 4
    print(oldz.shape) #(90191, 5)

    #stack the zillow tables
    temp = pd.concat([oldz, med_list_sqft_T], axis = 0) #(181597, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #89643

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(179286, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(89643, 5)
    dups['p_diff'] = (dups.xs('med_list_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_list_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_list_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 3%, which is kinda annoying but acceptable
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(91954, 5)
    tempa.drop('d', axis = 1).to_sql('zillow_med_list_sqft', engine, if_exists = 'replace', index = False)     


    #Median Listing Price Per Square Foot
    quandl_med_list_sqft = {'MLPFAH': 'All Homes',
                            'MLPFCC': 'Condo',
                            'MLPFDT': 'Duplex/Triplex',
                            'MLPF1B': 'One Bed',
                            'MLPF2B': 'Two Bed',
                            'MLPF3B': 'Three Bed',
                            'MLPF4B': 'Four Bed',
                            'MLPF5B': 'Many Bed',
                            'MLPFSF': 'Single Fam'}

    df_quandl_med_list_sqft = pd.DataFrame()

    for zip in df_state_city_zip_combined['state_city_zip_Name']:
        for key in quandl_med_list_sqft.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_med_list_sqft[key]
                a.rename(columns = {'Value': 'med_list_sqft'}, inplace = True)
                df_quandl_med_list_sqft = pd.concat([df_quandl_med_list_sqft, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_med_list_sqft[key]
                        a.rename(columns = {'Value': 'med_list_sqft'}, inplace = True)
                        df_quandl_med_list_sqft = pd.concat([df_quandl_med_list_sqft, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_med_list_sqft.shape) #(100437, 3)
    print(df_quandl_med_list_sqft.duplicated(keep = 'first').sum()) #8713
    df_quandl_med_list_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_list_sqft.shape) #(91724, 3)
    print(df_quandl_med_list_sqft.duplicated(keep = 'first').sum())
    df_quandl_med_list_sqft.index.name = 'Date'

    df_quandl_med_list_sqft.reset_index(inplace = True)
    df_quandl_med_list_sqft['d'] = 1

    #quandl home price index portion
    oldq = pd.read_sql('select * from quandl_med_list_sqft;', engine)
    oldq['d'] = 3
    print(oldq.shape) #(90126, 5)

    temp = pd.concat([oldq, df_quandl_med_list_sqft], axis = 0) #(181850, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #89543

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(179086, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(168974, 5)
    dups['p_diff'] = (dups.xs('med_list_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_list_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_list_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 8%, which is kinda annoying. 3 observations
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(92307, 5)
    tempa.drop('d', axis = 1).to_sql('quandl_med_list_sqft', engine, if_exists = 'replace', index = False)

    #datasource comparision
    #zillow home value index (ZHVI), we haven't changed this
    #do some work to compare the two database
    print(oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].shape) #(139, 4)
    print(zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].shape) #(157, 4)

    oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].to_csv('oz.csv', index = False)
    oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].to_csv('oq.csv', index = False)
    zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #combine these three tables: 1. oldz oldq, 2. zillow, 3. quandl
    #stack the four tables
    temp = pd.concat([oldz, oldq, med_list_sqft_T, df_quandl_med_list_sqft], axis = 0) #(363447, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #263781

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(363004, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(341656, 7)
    dups['p_diff'] = (dups.xs('med_list_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_list_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_list_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))].drop('d', axis = 1) #(99666, 4)


    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(100536, 4)

    temp1.to_sql('combined_med_list_sqft', engine, if_exists = 'replace', index = True)


def rental_per_sqft(df_state_city_zip_combined):
    #zillow rental per sqft portion
    med_rent_sqft = pd.DataFrame()

    for link in df_state_city_zip_combined['rental_sqft_url']:
        print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                med_rent_sqft = pd.concat([med_rent_sqft, temp], axis = 0) 
        except Exception as e:
            print(e)

    print(med_rent_sqft.shape) #(22040, 83)
    print(med_rent_sqft.duplicated(keep = 'first').sum()) #21880
    med_rent_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(med_rent_sqft.shape) #(160, 83)
    print(med_rent_sqft.duplicated(keep = 'first').sum())

    med_rent_sqft_T = med_rent_sqft.set_index(['Data Type','Region Name','Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'med_rent_sqft'})
    med_rent_sqft_T['Region Name'] = med_rent_sqft_T['Region Name'].apply(lambda x: str(x).zfill(5))
    med_rent_sqft_T.rename(columns = {'Data Type':'Home Type', 'Region Name': 'zip'}, inplace = True)
    med_rent_sqft_T.drop(['Region Type'], inplace = True, axis = 1)

    med_rent_sqft_T['d'] = 2

    #standard code to pull my data
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')

    #zillow home price index portion
    oldz = pd.read_sql('select * from med_rent_sqft;', engine) #remember to change
    oldz['d'] = 4
    print(oldz.shape) #(6505, 5)

    #stack the zillow tables
    temp = pd.concat([oldz, med_rent_sqft_T], axis = 0) #(13118, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #6375

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(12750, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(89643, 5)
    dups['p_diff'] = (dups.xs('med_rent_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_rent_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_rent_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 3%, which is kinda annoying but acceptable
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))
    

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(91954, 5)
    tempa.drop('d', axis = 1).to_sql('zillow_med_rent_sqft', engine, if_exists = 'replace', index = False)     


    quandl_med_rent_sqft = {'MRPFAH': 'All Homes',
                            'MRPFCC': 'Condo',
                            'MRPFDT': 'Duplex/Triplex',
                            'MRPFST': 'Studio',
                            'MRPF1B': 'One Bed',
                            'MRPF2B': 'Two Bed',
                            'MRPF3B': 'Three Bed',
                            'MRPF4B': 'Four Bed',
                            'MRPF5B': 'Many Bed',
                            'MRPFSF': 'Single Fam',
                            'MRPFMF': 'Multi Fam'}

    df_quandl_med_rent_sqft = pd.DataFrame()

    for zip in df_state_city_zip_combined['state_city_zip_Name']:
        for key in quandl_med_rent_sqft.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_med_rent_sqft[key]
                a.rename(columns = {'Value': 'med_rent_sqft'}, inplace = True)
                df_quandl_med_rent_sqft = pd.concat([df_quandl_med_rent_sqft, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_med_rent_sqft[key]
                        a.rename(columns = {'Value': 'med_rent_sqft'}, inplace = True)
                        df_quandl_med_rent_sqft = pd.concat([df_quandl_med_rent_sqft, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_med_rent_sqft.shape) #(9332, 3)
    print(df_quandl_med_rent_sqft.duplicated(keep = 'first').sum()) #1733
    df_quandl_med_rent_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_rent_sqft.shape) #(7599, 3)
    print(df_quandl_med_rent_sqft.duplicated(keep = 'first').sum())
    df_quandl_med_rent_sqft.index.name = 'Date'
    
    df_quandl_med_rent_sqft.reset_index(inplace = True)
    df_quandl_med_rent_sqft['d'] = 1

    #quandl home price index portion
    oldq = pd.read_sql('select * from quandl_med_rent_sqft;', engine)
    oldq['d'] = 3
    print(oldq.shape) #(7364, 5)

    temp = pd.concat([oldq, df_quandl_med_rent_sqft], axis = 0) #(14963, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #7057

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(14114, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(168974, 5)
    dups['p_diff'] = (dups.xs('med_rent_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_rent_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_rent_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 8%, which is kinda annoying. 3 observations
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(7906, 5)
    tempa.drop('d', axis = 1).to_sql('quandl_med_rent_sqft', engine, if_exists = 'replace', index = False)


    #datasource comparision
    #zillow home value index (ZHVI), we haven't changed this
    #do some work to compare the two database
    print(oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].shape) #(139, 4)
    print(zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].shape) #(120, 4)
    print(df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].shape) #(157, 4)

    oldz[(oldz['zip'] == '10025') & (oldz['Home Type'] == 'All Homes')].to_csv('oz.csv', index = False)
    oldq[(oldq['zip'] == '10025') & (oldq['Home Type'] == 'All Homes')].to_csv('oq.csv', index = False)
    zhvi_T[(zhvi_T['zip'] == '10025') & (zhvi_T['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    df_quandl_zhvi[(df_quandl_zhvi['zip'] == '10025') & (df_quandl_zhvi['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #combine these three tables: 1. oldz oldq, 2. zillow, 3. quandl
    #stack the four tables
    temp = pd.concat([oldz, oldq, med_rent_sqft_T, df_quandl_med_rent_sqft], axis = 0) #(28081, 5)

    #remove the duplicated, let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date', 'd'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #17821

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)] #(27582, 5)
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'd']).unstack(level = -1).reset_index() #(341656, 7)
    dups['p_diff'] = (dups.xs('med_rent_sqft', level=0, axis=1).max(axis = 1) - dups.xs('med_rent_sqft', level=0, axis=1).min(axis = 1)) / dups.xs('med_rent_sqft', level=0, axis=1).mean(axis = 1)
    print(dups['p_diff'].max()) #the largest difference is 7%, which is kinda annoying
    print(dups.sort_values(by = 'p_diff', ascending = False).head(20))

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))].drop('d', axis = 1) #(10260, 4)


    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(10653, 4)

    temp1.to_sql('combined_med_rent_sqft', engine, if_exists = 'replace', index = True)



def quandl_suppliment():
    #I need to look at quandl data to see if they are okay, and can they suppliment?
    #seems to be a lot missing
    #this is where quandl datapull starts


    #Median Sold Price -
    quandl_med_sold = {'MSPAH': 'All Homes'}

    df_quandl_med_sold = pd.DataFrame()

    for zip in temp['state_city_zip_Name']:
        for key in quandl_med_sold.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_med_sold[key]
                a.rename(columns = {'Value': 'med_sold'}, inplace = True)
                df_quandl_med_sold = pd.concat([df_quandl_med_sold, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_med_sold[key]
                        a.rename(columns = {'Value': 'med_sold'}, inplace = True)
                        df_quandl_med_sold = pd.concat([df_quandl_med_sold, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_med_sold.shape) #(58472, 3)
    print(df_quandl_med_sold.duplicated(keep = 'first').sum()) #1220
    df_quandl_med_sold.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_sold.shape) #(57252, 3)
    print(df_quandl_med_sold.duplicated(keep = 'first').sum())
    df_quandl_med_sold.index.name = 'Date'
    df_quandl_med_sold.to_sql('quandl_med_sold', engine, if_exists = 'replace', index = True) 


    #Median Sold Price Per Square Foot -
    quandl_med_sold_sqft = {'MSPFAH': 'All Homes',
                            'MSPFCO': 'Condo',
                            'MSPFSF': 'Single Fam'}

    df_quandl_med_sold_sqft = pd.DataFrame()

    for zip in temp['state_city_zip_Name']:
        for key in quandl_med_sold_sqft.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_med_sold_sqft[key]
                a.rename(columns = {'Value': 'med_sold_sqft'}, inplace = True)
                df_quandl_med_sold_sqft = pd.concat([df_quandl_med_sold_sqft, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_med_sold_sqft[key]
                        a.rename(columns = {'Value': 'med_sold_sqft'}, inplace = True)
                        df_quandl_med_sold_sqft = pd.concat([df_quandl_med_sold_sqft, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_med_sold_sqft.shape) #(223746, 3)
    print(df_quandl_med_sold_sqft.duplicated(keep = 'first').sum()) #82084
    df_quandl_med_sold_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_sold_sqft.shape) #(133478, 3) significantly less than last time
    print(df_quandl_med_sold_sqft.duplicated(keep = 'first').sum())
    df_quandl_med_sold_sqft.index.name = 'Date'
    df_quandl_med_sold_sqft.to_sql('quandl_med_sold_sqft', engine, if_exists = 'replace', index = True) 


    #Median Value Per Square Foot -

    df_quandl_med_val_sqft = pd.DataFrame()

    for zip in temp['state_city_zip_Name']:
        link = 'ZILLOW/Z' + zip + '_MSPFAH'
        try:
            a = quandl.get(link)
            a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
            a['zip'] = zip
            a['Home Type'] = 'All Homes'
            a.rename(columns = {'Value': 'med_val_sqft'}, inplace = True)
            df_quandl_med_val_sqft = pd.concat([df_quandl_med_val_sqft, a], axis = 0)
        except Exception as e:            
            if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                try: #maybe i should use a while loop??
                    print('error handling, try to pull with a different key \n')
                    current_key = quandl_init(current_key)
                    a = quandl.get(link)
                    a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                    a['zip'] = zip
                    a['Home Type'] = 'All Homes'
                    a.rename(columns = {'Value': 'med_val_sqft'}, inplace = True)
                    df_quandl_med_val_sqft = pd.concat([df_quandl_med_val_sqft, a], axis = 0)
                except Exception as o:
                    print(link + 'second error' + str(o))
            print(link + str(e))
            
    print(df_quandl_med_val_sqft.shape) #(153766, 3)
    print(df_quandl_med_val_sqft.duplicated(keep = 'first').sum()) #67854
    df_quandl_med_val_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_med_val_sqft.shape) #(85912, 3) significantly less than before
    print(df_quandl_med_val_sqft.duplicated(keep = 'first').sum())
    df_quandl_med_val_sqft.index.name = 'Date'
    df_quandl_med_val_sqft.to_sql('quandl_med_val_sqft', engine, if_exists = 'replace', index = True) 

    #Median Rental Price Per Square -

    
     

    #Zillow Rental Index Per Square Foot

    df_quandl_zri_sqft = pd.DataFrame()

    for zip in temp['state_city_zip_Name']:
        link = 'ZILLOW/Z' + zip + '_ZRIFAH'
        try:
            a = quandl.get(link)
            a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
            a['zip'] = zip
            a['Home Type'] = 'All Homes'
            a.rename(columns = {'Value': 'zri_sqft'}, inplace = True)
            df_quandl_zri_sqft = pd.concat([df_quandl_zri_sqft, a], axis = 0)
        except Exception as e:            
            if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                try: #maybe i should use a while loop??
                    print('error handling, try to pull with a different key \n')
                    current_key = quandl_init(current_key)
                    a = quandl.get(link)
                    a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                    a['zip'] = zip
                    a['Home Type'] = 'All Homes'
                    a.rename(columns = {'Value': 'zri_sqft'}, inplace = True)
                    df_quandl_zri_sqft = pd.concat([df_quandl_zri_sqft, a], axis = 0)
                except Exception as o:
                    print(link + 'second error' + str(o))
            print(link + str(e))

    print(df_quandl_zri_sqft.shape) #(55366, 3)
    print(df_quandl_zri_sqft.duplicated(keep = 'first').sum()) #16761
    df_quandl_zri_sqft.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_zri_sqft.shape) #(38605, 3)
    print(df_quandl_zri_sqft.duplicated(keep = 'first').sum())
    df_quandl_zri_sqft.index.name = 'Date'
    df_quandl_zri_sqft.to_sql('quandl_zri_sqft', engine, if_exists = 'replace', index = True) 

    #Median Rental Price
    quandl_rent_p = {'MRPAH': 'All Homes',
                     'MRPCC': 'Condo',
                     'MRPDT': 'Duplex/Triplex',
                     'MR51B': 'Many Bed',
                     'MRP4B': 'Four Bed',
                     'MRPMF': 'Multi Fam',
                     'MRP1B': 'One Bed',
                     'MRPSF': 'Single Fam',
                     'MRPST': 'Studio',
                     'MRP3B': 'Three Bed',
                     'MRP2B': 'Two Bed'}

    df_quandl_rent_p = pd.DataFrame()

    for zip in temp['state_city_zip_Name']:
        for key in quandl_rent_p.keys():
            link = 'ZILLOW/Z' + zip + '_' + key
            try:
                a = quandl.get(link)
                a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                a['zip'] = zip
                a['Home Type'] = quandl_rent_p[key]
                a.rename(columns = {'Value': 'rent_p'}, inplace = True)
                df_quandl_rent_p = pd.concat([df_quandl_rent_p, a], axis = 0)
            except Exception as e:            
                if str(e) == '(Status 429) (Quandl Error QELx04) You have exceeded the API speed limit of 2000 calls per 10 minutes. Please slow down your requests.':
                    try: #maybe i should use a while loop??
                        print('error handling, try to pull with a different key \n')
                        current_key = quandl_init(current_key)
                        a = quandl.get(link)
                        a.index = [datetime.datetime(i.year, i.month, 1) for i in a.index]
                        a['zip'] = zip
                        a['Home Type'] = quandl_rent_p[key]
                        a.rename(columns = {'Value': 'rent_p'}, inplace = True)
                        df_quandl_rent_p = pd.concat([df_quandl_rent_p, a], axis = 0)
                    except Exception as o:
                        print(link + 'second error' + str(o))
                print(link + str(e))

    print(df_quandl_rent_p.shape) #(49304, 3)
    print(df_quandl_rent_p.duplicated(keep = 'first').sum()) #28026
    df_quandl_rent_p.drop_duplicates(keep = 'first', inplace = True)
    print(df_quandl_rent_p.shape) #(21278, 3)
    print(df_quandl_rent_p.duplicated(keep = 'first').sum())
    df_quandl_rent_p.index.name = 'Date'
    df_quandl_rent_p.to_sql('quandl_rent_p', engine, if_exists = 'replace', index = True) 

    #there's also an alternate datasource for quandl, let's take a quick look as well
    #doesn't seem like those files exist anymore







    #zillow median list portion
    med_list = pd.read_sql('select * from med_list;', engine)
    print(med_list.shape) #(84254, 5)

    quandl_med_list = pd.read_sql('select * from quandl_med_list;', engine)
    print(quandl_med_list.shape) #(60279, 4)

    #do some work to compare the two database
    print(med_list[(med_list['zip'] == '10025') & (med_list['Home Type'] == 'All Homes')].shape) #(64, 4)
    print(quandl_med_list[(quandl_med_list['zip'] == '10025') & (quandl_med_list['Home Type'] == 'All Homes')].shape) #(43, 4)

    med_list[(med_list['zip'] == '10025') & (med_list['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    quandl_med_list[(quandl_med_list['zip'] == '10025') & (quandl_med_list['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #stack the two tables
    temp = pd.concat([med_list, quandl_med_list], axis = 0) #(144533, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].shape) #(107, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].duplicated(keep = 'first').sum()) #39

    #remove the duplicated
    print(temp.duplicated(keep = 'first').sum()) #37695
    temp.drop_duplicates(keep = 'first', inplace = True) #(106838, 4)
    temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].to_csv('c.csv', index = False)

    #let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = False).sum()) #30528 there are still a lot of dups, I may need to write code to figure out what the differences are

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)]
    dups['ab'] = dups[['Date', 'Home Type', 'zip']].duplicated(keep = 'first') 
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'ab']).unstack(level = -1).reset_index() #(15264, 4)
    dups['diff'] = abs(dups.xs(False, level=1, axis=1) / dups.xs(True, level=1, axis=1) - 1)
    print(dups['diff'].max()) #looks like the differences are okay

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(91574, 4)

    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(95880, 4)

    temp1.to_sql('combined_med_list', engine, if_exists = 'replace', index = True) 



    #zillow median list per sqft portion
    med_list_sqft = pd.read_sql('select * from med_list_sqft;', engine)
    print(med_list_sqft.shape) #(90191, 4)

    quandl_med_list_sqft = pd.read_sql('select * from quandl_med_list_sqft;', engine)
    print(quandl_med_list_sqft.shape) #(90126, 4) #for some reason this looks like the rounded version

    #do some work to compare the two database
    print(med_list_sqft[(med_list_sqft['zip'] == '10025') & (med_list_sqft['Home Type'] == 'All Homes')].shape) #(90, 4)
    print(quandl_med_list_sqft[(quandl_med_list_sqft['zip'] == '10025') & (quandl_med_list_sqft['Home Type'] == 'All Homes')].shape) #(86, 4)

    med_list_sqft[(med_list_sqft['zip'] == '10025') & (med_list_sqft['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    quandl_med_list_sqft[(quandl_med_list_sqft['zip'] == '10025') & (quandl_med_list_sqft['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #seems like we need to round some stuff
    quandl_med_list_sqft['med_list_sqft'] = quandl_med_list_sqft['med_list_sqft'].round(0)

    #stack the two tables
    temp = pd.concat([quandl_med_list_sqft, med_list_sqft], axis = 0) #(180317, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].shape) #(176, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].duplicated(keep = 'first').sum()) #86

    #remove the duplicated
    print(temp.duplicated(keep = 'first').sum()) #82274
    temp.drop_duplicates(keep = 'first', inplace = True)
    temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].to_csv('c.csv', index = False)

    #let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) #163 there are still dups
    temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)].to_csv('dups.csv', index = False) #spot checked and seems like they are within reason
    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))]
    print(tempa[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum())

    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(98799, 4)

    temp1.to_sql('combined_med_list_sqft', engine, if_exists = 'replace', index = True) 






    #zillow rental index portion
    zri = pd.read_sql('select * from zri;', engine)
    print(zri.shape) #(218084, 5)

    quandl_zri = pd.read_sql('select * from quandl_zri;', engine)
    print(quandl_zri.shape) #(1263, 4)

    #do some work to compare the two database
    print(zri[(zri['zip'] == '10025') & (zri['Home Type'] == 'All Homes')].shape) #(67, 4)
    print(quandl_zri[(quandl_zri['zip'] == '10025') & (quandl_zri['Home Type'] == 'All Homes')].shape) #(63, 4)

    zri[(zri['zip'] == '10025') & (zri['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    quandl_zri[(quandl_zri['zip'] == '10025') & (quandl_zri['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)

    #stack the two tables
    zri.rename(columns = {'zrvi': 'zri'}, inplace = True)
    temp = pd.concat([zri, quandl_zri], axis = 0) #(396874, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].shape) #(130, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].duplicated(keep = 'first').sum()) #63

    #remove the duplicated
    print(temp.duplicated(keep = 'first').sum()) #48841
    temp.drop_duplicates(keep = 'first', inplace = True)
    temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].to_csv('c.csv', index = False)

    #let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date'], inplace = True) #(348033, 4)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first').sum()) 

    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in temp['zip'].unique():
        for home_type in temp[temp['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = temp[(temp['zip'] == zipcode) & (temp['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(364536, 4)


    temp1[temp1.interp == True].head(20)
    temp1[(temp1['Home Type'] == 'All Homes Multi Fam') & (temp1['zip'] == '10001')].tail(20)

    temp1.to_sql('combined_zri', engine, if_exists = 'replace', index = True) 



    #zillow rental per sqft portion
    med_rent_sqft = pd.read_sql('select * from med_rent_sqft;', engine)
    print(med_rent_sqft.shape) #(6505, 5)

    quandl_med_rent_sqft = pd.read_sql('select * from quandl_med_rent_sqft;', engine)
    print(quandl_med_rent_sqft.shape) #(7364, 4)

    #do some work to compare the two database
    print(med_rent_sqft[(med_rent_sqft['zip'] == '10025') & (med_rent_sqft['Home Type'] == 'All Homes')].shape) #(45, 4)
    print(quandl_med_rent_sqft[(quandl_med_rent_sqft['zip'] == '10025') & (quandl_med_rent_sqft['Home Type'] == 'All Homes')].shape) #(38, 4)

    med_rent_sqft[(med_rent_sqft['zip'] == '10025') & (med_rent_sqft['Home Type'] == 'All Homes')].to_csv('z.csv', index = False)
    quandl_med_rent_sqft[(quandl_med_rent_sqft['zip'] == '10025') & (quandl_med_rent_sqft['Home Type'] == 'All Homes')].to_csv('q.csv', index = False)


    #stack the two tables
    temp = pd.concat([med_rent_sqft, quandl_med_rent_sqft], axis = 0) #(13869, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].shape) #(83, 4)
    print(temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].duplicated(keep = 'first').sum()) #0

    #remove the duplicated
    print(temp.duplicated(keep = 'first').sum()) #197
    temp.drop_duplicates(keep = 'first', inplace = True) #(13672, 4)
    temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].to_csv('c.csv', index = False)

    #let's look at if there are still duplicated columns
    temp.sort_values(by = ['zip', 'Home Type', 'Date'], inplace = True)
    print(temp[['Date', 'Home Type', 'zip']].duplicated(keep = False).sum()) #7746 there are still a lot of dups, I may need to write code to figure out what the differences are

    dups = temp[temp[['Date', 'Home Type', 'zip']].duplicated(keep = False)]
    dups['ab'] = dups[['Date', 'Home Type', 'zip']].duplicated(keep = 'first') 
    dups = dups.set_index(['Date', 'Home Type', 'zip', 'ab']).unstack(level = -1).reset_index() #(15264, 4)
    dups['diff'] = abs(dups.xs(False, level=1, axis=1) / dups.xs(True, level=1, axis=1) - 1)
    print(dups['diff'].max()) #looks like the differences are okay

    tempa = temp[(~temp[['Date', 'Home Type', 'zip']].duplicated(keep = 'first'))] #(9799, 4)

    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    print(temp1.shape) #(10276, 4)
    temp1[temp1.interp == True].head()
    temp1[(temp1['Home Type'] == 'Duplex/Triplex') & (temp1['zip'] == '10001')]
    temp1.to_sql('combined_med_rent_sqft', engine, if_exists = 'replace', index = True) 

def irs_data():
    years = [2009, 2010, 2011, 2012, 2013, 2014]
    combined = pd.DataFrame()
    for year in years:
        df = pd.read_csv('E:\WinUser\Documents\Python Code\Zillow Fun\{}zpallnoagi.csv'.format(str(year)[-2:]))
        df.rename(columns = {'N1': 'num R', 'A00100': 'AGI', 'A00200': 'SW', 'A00300': 'TI', 'A00600': 'OD',
                             'A00650': 'QD', 'A00900': 'BPI', 'A01000': 'CG', 'A01400': 'IRAD', 'A01700': 'TPA',
                             'A02500': 'TSSB', 'A03300': 'SRP'}, inplace = True)
        
        df['avg AGI'] = df['AGI'] * 1000 / df['num R']
        df['avg SW'] = df['SW'] * 1000 / df['num R']
        df['avg OI'] = (df['TI'] + df['OD'] + df['QD'] + df['BPI'] + df['CG'] + df['IRAD'] + df['TPA'] + df['TSSB'] + df['SRP']) * 1000 / df['num R']
        df['ZIPCODE'] = df['ZIPCODE'].apply(lambda x: str(x).zfill(5))
        df['year'] = year
        combined = pd.concat([combined, df[['STATE', 'ZIPCODE', 'year', 'num R', 'avg AGI', 'avg SW', 'avg OI']]], axis = 0)

    zip_summary = combined[combined['ZIPCODE'] != '00000']
    state_summary = combined[combined['ZIPCODE'] == '00000']

    zip_summary['num R z'] = (zip_summary['num R'] - zip_summary[zip_summary['year'] == max(years)]['num R'].mean()) / zip_summary[zip_summary['year'] == max(years)]['num R'].std()
    zip_summary['avg AGI z'] = (zip_summary['avg AGI'] - zip_summary[zip_summary['year'] == max(years)]['avg AGI'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg AGI'].std()
    zip_summary['avg SW z'] = (zip_summary['avg SW'] - zip_summary[zip_summary['year'] == max(years)]['avg SW'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg SW'].std()
    zip_summary['avg OI z'] = (zip_summary['avg OI'] - zip_summary[zip_summary['year'] == max(years)]['avg OI'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg OI'].std()

    zip_summary.sort_values(by = ['ZIPCODE', 'year'], ascending = True, inplace = True)
    zip_summary['num R pchg'] = zip_summary.groupby('ZIPCODE')['num R'].pct_change()
    zip_summary['avg AGI pchg'] = zip_summary.groupby('ZIPCODE')['avg AGI'].pct_change()
    zip_summary['avg OI pchg'] = zip_summary.groupby('ZIPCODE')['avg OI'].pct_change()
    zip_summary['avg SW pchg'] = zip_summary.groupby('ZIPCODE')['avg SW'].pct_change()

    state_summary['num R z'] = (state_summary['num R'] - state_summary[state_summary['year'] == max(years)]['num R'].mean()) / state_summary[state_summary['year'] == max(years)]['num R'].std()
    state_summary['avg AGI z'] = (state_summary['avg AGI'] - state_summary[state_summary['year'] == max(years)]['avg AGI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg AGI'].std()
    state_summary['avg SW z'] = (state_summary['avg SW'] - state_summary[state_summary['year'] == max(years)]['avg SW'].mean()) / state_summary[state_summary['year'] == max(years)]['avg SW'].std()
    state_summary['avg OI z'] = (state_summary['avg OI'] - state_summary[state_summary['year'] == max(years)]['avg OI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg OI'].std()

    state_summary.sort_values(by = ['STATE', 'year'], ascending = True, inplace = True)
    state_summary['num R pchg'] = state_summary.groupby('STATE')['num R'].pct_change()
    state_summary['avg AGI pchg'] = state_summary.groupby('STATE')['avg AGI'].pct_change()
    state_summary['avg SW pchg'] = state_summary.groupby('STATE')['avg SW'].pct_change()
    state_summary['avg OI pchg'] = state_summary.groupby('STATE')['avg OI'].pct_change()

    #here we are trying to predict what's going to happen 2-3 years into the future
    zip_summary['year y3'] = zip_summary['year'] + 3
    zip_summary['year y2'] = zip_summary['year'] + 2
    
    return state_summary, zip_summary

def quandl_init(key):
    Quandlkey = ['7ieY8tq_kjzWx2-DiyGx', 'XrVxmKtfg2Fo3FG_NmtC', 'Jh3CAbmwaNP7YoqAN4FK']
    if key == None:
        quandl.ApiConfig.api_key = Quandlkey[0]
        return Quandlkey[0]
    else:
        if Quandlkey.index(key) == 2:
            quandl.ApiConfig.api_key = Quandlkey[0]
            return Quandlkey[0]
        else:
            quandl.ApiConfig.api_key = Quandlkey[Quandlkey.index(key) + 1]
            return Quandlkey[Quandlkey.index(key) + 1]


#federal funds rate
def get_fed_rate(draw = False):
    current_key = quandl_init(None)
    temp = quandl.get('FED/RIFSPFF_N_M')
    temp.index = [datetime.datetime(i.year, i.month, 1) for i in temp.index]
    temp.rename(columns = {'Value': 'Fed Rate'}, inplace = True)
    temp['Fed Rate pchg'] = temp['Fed Rate'].pct_change()
    temp['year'] = temp.index.year
    temp['date m1'] = temp.index.shift(1, freq = 'MS')
    temp['date m2'] = temp.index.shift(2, freq = 'MS')
    temp['date m3'] = temp.index.shift(3, freq = 'MS')
    temp['date y1'] = temp.index.shift(12, freq = 'MS')
    temp_annual = temp[['year', 'Fed Rate']].groupby(['year'])[['Fed Rate']].mean()
    temp_annual['Fed Rate pchg'] = temp_annual['Fed Rate'].pct_change()
    temp_annual['year y0'] = temp_annual.index
    temp_annual['year y1'] = temp_annual.index + 1
    temp_annual['year y2'] = temp_annual.index + 2
    temp_annual['year y3'] = temp_annual.index + 3
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp['Fed Rate'])
        axarr[0].set_title('monthly')
        axarr[1].plot(temp_annual['Fed Rate'])
        axarr[1].set_title('annual mean')
        plt.tight_layout()
        plt.show()
    return temp, temp_annual


#mortgage rate
def get_mtg_rate(draw = False):

    #'http://www.freddiemac.com/pmms/docs/30yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/15yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/5yr_pmmsmnth.xls'
    
    link = r'mortgage rates.xlsx'
    xls = pd.ExcelFile(link)
    temp = xls.parse(0, header = 0)
    temp['year'].fillna(method = 'ffill', inplace = True)
    temp['day'] = 1
    temp['Date'] = pd.to_datetime(temp[['year','month','day']])
    temp.drop(['month','day'], axis = 1, inplace = True)
    temp.set_index('Date', inplace = True)
    temp['30 FRM Rate pchg'] = temp['30 FRM Rate'].pct_change()
    temp['date m1'] = temp.index.shift(1, freq = 'MS')
    temp['date m2'] = temp.index.shift(2, freq = 'MS')
    temp['date m3'] = temp.index.shift(3, freq = 'MS')
    temp['date y1'] = temp.index.shift(12, freq = 'MS')
    temp_annual = temp.groupby('year')[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']].mean()
    temp_annual['30 FRM Rate pchg'] = temp_annual['30 FRM Rate'].pct_change()
    temp_annual['year y0'] = temp_annual.index.astype(np.int32)
    temp_annual['year y1'] = temp_annual.index.astype(np.int32) + 1
    temp_annual['year y2'] = temp_annual.index.astype(np.int32) + 2
    temp_annual['year y3'] = temp_annual.index.astype(np.int32) + 3
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[0].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[0].set_title('monthly')
        axarr[1].plot(temp_annual[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[1].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[1].set_title('annual')
        plt.tight_layout()
        plt.show()
    return temp, temp_annual
    
#market movements
def get_market(draw = False):
    dow_link = '^DJI.csv'
    nasdaq_link = '^IXIC.csv'
    sp500_link = '^GSPC.csv'
    rez_link = 'REZ.csv'
    iyr_link = 'IYR.csv'
    vnq_link = 'VNQ.csv'
    schh_link = 'SCHH.csv'
    #indexes
    dow = pd.read_csv(dow_link).set_index('Date').add_suffix('_d')
    dow.index = pd.to_datetime(dow.index)
    nasdaq = pd.read_csv(nasdaq_link).set_index('Date').add_suffix('_n')
    nasdaq.index = pd.to_datetime(nasdaq.index)
    sp500 = pd.read_csv(sp500_link).set_index('Date').add_suffix('_s')
    sp500.index = pd.to_datetime(sp500.index)
    #REITs
    rez = pd.read_csv(rez_link).set_index('Date').add_suffix('_rez')
    rez.index = pd.to_datetime(rez.index)
    iyr = pd.read_csv(iyr_link).set_index('Date').add_suffix('_iyr')
    iyr.index = pd.to_datetime(iyr.index)
    vnq = pd.read_csv(vnq_link).set_index('Date').add_suffix('_vnq')
    vnq.index = pd.to_datetime(vnq.index)
    schh = pd.read_csv(schh_link).set_index('Date').add_suffix('_schh')
    schh.index = pd.to_datetime(schh.index)

    temp = dow.merge(nasdaq, how = 'left', left_index = True, right_index = True).merge(sp500, how = 'left', left_index = True, right_index = True)
    temp = temp.merge(rez, how = 'left', left_index = True, right_index = True).merge(iyr, how = 'left', left_index = True, right_index = True).merge(vnq, how = 'left', left_index = True, right_index = True).merge(schh, how = 'left', left_index = True, right_index = True)
    temp['year'] = temp.index.year
    temp['month'] = temp.index.month
    temp['day'] = 1
    temp['as_of_dt'] = pd.to_datetime(temp[['year', 'month', 'day']])
    temp.rename(columns = {'Adj Close_d': 'DOW',
                           'Adj Close_n': 'NASDAQ',
                           'Adj Close_s': 'SP500',
                           'Adj Close_rez': 'REZ',
                           'Adj Close_iyr': 'IYR',
                           'Adj Close_vnq': 'VNQ',
                           'Adj Close_schh': 'SCHH'}, inplace = True)
    temp_month = temp.groupby('as_of_dt')[['DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].mean().add_suffix(' avg').merge(temp.groupby('as_of_dt')[['DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].mean().pct_change().add_suffix(' pchg'), how = 'left', left_index = True, right_index = True)
    
    temp_month['date m1'] = temp_month.index.shift(1, freq = 'MS')
    temp_month['date m2'] = temp_month.index.shift(2, freq = 'MS')
    temp_month['date m3'] = temp_month.index.shift(3, freq = 'MS')
    temp_month['date y1'] = temp_month.index.shift(12, freq = 'MS')
    temp_annual = (temp[['year','DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].groupby(['year'])[['DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].mean()).merge((temp[['year','DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].groupby(['year'])[['DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].last() / temp[['year','DOW','NASDAQ','SP500','REZ','IYR','VNQ','SCHH']].groupby(['year'])[['DOW', 'NASDAQ', 'SP500']].first() - 1), how = 'left', left_index = True, right_index = True, suffixes = (' avg', ' ann_perf'))

    temp_annual[['DOW pchg','NASDAQ pchg','SP500 pchg','REZ pchg','IYR pchg','VNQ pchg','SCHH pchg']] = temp_annual[['DOW avg','NASDAQ avg','SP500 avg','REZ avg','IYR avg','VNQ avg','SCHH avg']].pct_change()
    temp_annual['year_p0'] = temp_annual.index
    temp_annual['year_p1'] = temp_annual.index + 1
    temp_annual['year_p2'] = temp_annual.index + 2
    temp_annual['year_p3'] = temp_annual.index + 3
    if draw:
        f, axarr = plt.subplots(3, sharex = False)
        axarr[0].plot(temp[['DOW','SP500','NASDAQ','REZ','IYR','VNQ','SCHH']])
        axarr[0].legend(['DOW','SP500','NASDAQ','REZ','IYR','VNQ','SCHH'])
        axarr[0].set_title('daily')
        axarr[1].plot(temp_month[['DOW avg','SP500 avg','NASDAQ avg','REZ avg','IYR avg','VNQ avg','SCHH avg']])
        axarr[1].legend(['DOW','SP500','NASDAQ','REZ','IYR','VNQ','SCHH'])
        axarr[1].set_title('monthly')
        axarr[2].plot(temp_annual[['DOW avg','SP500 avg','NASDAQ avg','REZ avg','IYR avg','VNQ avg','SCHH avg']])
        axarr[2].legend(['DOW','SP500','NASDAQ','REZ','IYR','VNQ','SCHH'])
        axarr[2].set_title('annual')
        plt.tight_layout()
        plt.show()

    return temp_month, temp_annual


if __name__ == '__main__':
    #let's look at these data and work with them on tableau
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    combined_zhvi = pd.read_sql('select * from combined_zhvi;', engine) #(1318872, 5)
    combined_zri = pd.read_sql('select * from combined_zri;', engine) #(364536, 5)
    combined_med_list_sqft = pd.read_sql('select * from combined_med_list_sqft;', engine) #(98799, 5)
    combined_med_rent_sqft = pd.read_sql('select * from combined_med_rent_sqft;', engine) #(10276, 5)

    temp = combined_zhvi[['index', 'Home Type', 'zhvi', 'zip']].merge(combined_zri[['index', 'Home Type', 'zri', 'zip']],
                                                                      how = 'outer',
                                                                      left_on = ['index', 'Home Type', 'zip'],
                                                                      right_on = ['index', 'Home Type', 'zip']) #(1463471, 5)

    temp = temp.merge(combined_med_list_sqft[['index', 'Home Type', 'med_list_sqft', 'zip']],
                      how = 'outer', left_on = ['index', 'Home Type', 'zip'],
                      right_on = ['index', 'Home Type', 'zip']) 

    temp = temp.merge(combined_med_rent_sqft[['index', 'Home Type', 'med_rent_sqft', 'zip']],
                      how = 'outer', left_on = ['index', 'Home Type', 'zip'],
                      right_on = ['index', 'Home Type', 'zip']) #1478989


    temp['cap_rate'] = temp['zri'] * 12 / temp['zhvi']
    temp['cap_rate_sqft'] = temp['med_rent_sqft'] * 12 / temp['med_list_sqft']

    #temp['zhvi_AG'] = temp['zhvi'].pct_change(periods = 12)
    #temp['zri_AG'] = temp['zri'].pct_change(periods = 12)
    #temp['ri_hvi_AG'] = temp['ri_hvi'].pct_change(periods = 12)

    print(temp.shape) #(1511181, 9)

# we can also think about using mean and std that's within the past year
    recent = pd.DataFrame()
    recent['zhvi'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['zhvi']
    recent['zri'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['zri']
    recent['med_list_sqft'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['med_list_sqft']
    recent['med_rent_sqft'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['med_rent_sqft']
    recent['cap_rate'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['cap_rate']
    recent['cap_rate_sqft'] = temp[(temp['index'] > (temp['index'].max() - relativedelta(months = 12))) & (temp['index'] <= (temp['index'].max()))]['cap_rate_sqft']

    temp['zhvi_z'] = (temp['zhvi'] - recent['zhvi'].mean()) / recent['zhvi'].std()
    temp['zri_z'] = (temp['zri'] - recent['zri'].mean()) / recent['zri'].std()
    temp['med_list_sqft_z'] = (temp['med_list_sqft'] - recent['med_list_sqft'].mean()) / recent['med_list_sqft'].std()
    temp['med_rent_sqft_z'] = (temp['med_rent_sqft'] - recent['med_rent_sqft'].mean()) / recent['med_rent_sqft'].std()
    temp['cap_rate_z'] = (temp['cap_rate'] - recent['cap_rate'].mean()) / recent['cap_rate'].std()
    temp['cap_rate_sqft_z'] = (temp['cap_rate_sqft'] - recent['cap_rate_sqft'].mean()) / recent['cap_rate_sqft'].std()
    temp['year'] = temp['index'].dt.year
    temp[(temp['zip'] == '10025') & (temp['Home Type'] == 'All Homes')].tail(20)
    temp[['index', 'Home Type', 'zip', 'zhvi']].dropna().to_csv('zhvi.csv', index = False)

    print(abs(temp['zri_z'] >= 3.5).sum() / (~(np.isnan(temp['zri_z']))).sum(), temp['zri_z'].min()) #0.00876250869082 -1.36850495982
    print(abs(temp['zhvi_z'] >= 3.5).sum() / (~(np.isnan(temp['zhvi_z']))).sum(), temp['zhvi_z'].min()) #0.00604022137633 -0.829526668371
    print(abs(temp['med_list_sqft_z'] >= 3.5).sum() / (~(np.isnan(temp['med_list_sqft_z']))).sum(), temp['med_list_sqft_z'].min()) #0.0108518341689 -0.69305876602
    print(abs(temp['med_rent_sqft_z'] >= 3.5).sum() / (~(np.isnan(temp['med_rent_sqft_z']))).sum(), temp['med_rent_sqft_z'].min()) #0.0018774054257 -1.8475130686
    print(abs(temp['cap_rate_z'] >= 3.5).sum() / (~(np.isnan(temp['cap_rate_z']))).sum(), temp['cap_rate_z'].min()) #0.00712365473699 -2.30276848417
    print(abs(temp['cap_rate_sqft_z'] >= 3.5).sum() / (~(np.isnan(temp['cap_rate_sqft_z']))).sum(), temp['cap_rate_sqft_z'].min()) #0.0209375 -0.644353381805

    #for some reason tableau can only take a million rows
    print(temp[temp['index'].dt.year > 2006].shape) #(914992, 16)
    #temp[temp['index'].dt.year > 2006].to_csv('everything.csv', index = False)

    #get external data
    state_summary, zip_summary = irs_data()
    fed_rate, fed_rate_annual = get_fed_rate()
    mortgage_rate, mortgage_rate_annual = get_mtg_rate()
    market, market_annual = get_market()
    
    #summarize @ year
    
    annual = temp[temp['Home Type'] == 'All Homes'].groupby(['zip', 'Home Type', 'year'])[['zhvi', 'zri', 'zhvi_z', 'zri_z',
                                                                                           'med_list_sqft', 'med_rent_sqft',
                                                                                           'cap_rate', 'cap_rate_z', 'cap_rate_sqft']].mean()

                                                                                       
    annual['zhvi_annual_pchg'] = annual['zhvi'].groupby(level = [0,1]).pct_change()
    annual['zri_annual_pchg'] = annual['zri'].groupby(level = [0,1]).pct_change()
    annual['cap_rate_annual_pchg'] = annual['cap_rate'].groupby(level = [0,1]).pct_change()
        
    annual['zhvi y1'] = annual['zhvi'].groupby(level = [0,1]).shift(1)
    annual['zri y1'] = annual['zri'].groupby(level = [0,1]).shift(1)
    annual['cap_rate y1'] = annual['cap_rate'].groupby(level = [0,1]).shift(1)

    annual['zhvi_z y1'] = annual['zhvi_z'].groupby(level = [0,1]).shift(1)
    annual['zri_z y1'] = annual['zri_z'].groupby(level = [0,1]).shift(1)
    annual['cap_rate_z y1'] = annual['cap_rate_z'].groupby(level = [0,1]).shift(1)

    annual['zhvi_annual_cphg y1'] = annual['zhvi_annual_pchg'].groupby(level = [0,1]).shift(1)
    annual['zri_annual_pchg y1'] = annual['zri_annual_pchg'].groupby(level = [0,1]).shift(1)
    annual['cap_rate_annual_pchg y1'] = annual['cap_rate_annual_pchg'].groupby(level = [0,1]).shift(1)

    annual['zhvi y2'] = annual['zhvi'].groupby(level = [0,1]).shift(2)
    annual['zri y2'] = annual['zri'].groupby(level = [0,1]).shift(2)
    annual['cap_rate y2'] = annual['cap_rate'].groupby(level = [0,1]).shift(2)

    annual['zhvi_z y2'] = annual['zhvi_z'].groupby(level = [0,1]).shift(2)
    annual['zri_z y2'] = annual['zri_z'].groupby(level = [0,1]).shift(2)
    annual['cap_rate_z y2'] = annual['cap_rate_z'].groupby(level = [0,1]).shift(2)

    annual['zhvi_annual_pchg y2'] = annual['zhvi_annual_pchg'].groupby(level = [0,1]).shift(2)
    annual['zri_annual_pchg y2'] = annual['zri_annual_pchg'].groupby(level = [0,1]).shift(2)
    annual['cap_rate_annual_pchg y2'] = annual['cap_rate_annual_pchg'].groupby(level = [0,1]).shift(2)
        
        
    annual.reset_index(inplace = True)
       
    annual = annual.merge(zip_summary[['ZIPCODE', 'year y3', 'num R', 'avg AGI', 'avg SW', 'avg OI']].add_suffix(' y3'),
                          how = 'left', left_on = ['zip', 'year'], right_on = ['ZIPCODE y3', 'year y3 y3']).drop(['ZIPCODE y3', 'year y3 y3'], axis = 1)

    annual['avg AGI pchg y3'] = annual.groupby(['zip', 'Home Type'])['avg AGI y3'].pct_change()
    annual['avg SW pchg y3'] = annual.groupby(['zip', 'Home Type'])['avg SW y3'].pct_change()
    annual['avg OI pchg y3'] = annual.groupby(['zip', 'Home Type'])['avg OI y3'].pct_change()
    annual['num R pchg y3'] = annual.groupby(['zip', 'Home Type'])['num R y3'].pct_change()

    annual = annual.merge(zip_summary[['ZIPCODE', 'year y2', 'num R', 'avg AGI', 'avg SW', 'avg OI']].add_suffix(' y2'),
                          how = 'left', left_on = ['zip', 'year'], right_on = ['ZIPCODE y2', 'year y2 y2']).drop(['ZIPCODE y2', 'year y2 y2'], axis = 1)

    annual['avg AGI pchg y2'] = annual.groupby(['zip', 'Home Type'])['avg AGI y2'].pct_change()
    annual['avg SW pchg y2'] = annual.groupby(['zip', 'Home Type'])['avg SW y2'].pct_change()
    annual['avg OI pchg y2'] = annual.groupby(['zip', 'Home Type'])['avg OI y2'].pct_change()
    annual['num R pchg y2'] = annual.groupby(['zip', 'Home Type'])['num R y2'].pct_change()

    
    annual['zhvi_log'] = np.log(temp['zhvi'])
    annual['zri_log'] = np.log(temp['zri'])
    annual['cap_rate_log'] = np.log(temp['cap_rate'])
    
    #summarize @ month  
    monthly = temp[temp['Home Type'] == 'All Homes'] #(183113, 16)
    monthly['zhvi_log'] = np.log(monthly['zhvi'])
    monthly['zri_log'] = np.log(monthly['zri'])
    monthly['cap_rate_log'] = np.log(monthly['cap_rate'])
    monthly['zhvi_month_pchg'] = monthly.groupby(['zip','Home Type'])['zhvi'].pct_change()
    monthly['zri_month_pchg'] = monthly.groupby(['zip','Home Type'])['zri'].pct_change()
    monthly['cap_rate_month_pchg'] = monthly.groupby(['zip','Home Type'])['cap_rate'].pct_change()
    
    monthly['zhvi m1'] = monthly.groupby(['zip', 'Home Type'])['zhvi'].shift(1)
    monthly['zhvi m2'] = monthly.groupby(['zip', 'Home Type'])['zhvi'].shift(2)
    monthly['zhvi m3'] = monthly.groupby(['zip', 'Home Type'])['zhvi'].shift(3)
    monthly['zhvi m4'] = monthly.groupby(['zip', 'Home Type'])['zhvi'].shift(4)
    monthly['zhvi y1'] = monthly.groupby(['zip', 'Home Type'])['zhvi'].shift(12)
    monthly['zri m1'] = monthly.groupby(['zip', 'Home Type'])['zri'].shift(1)
    monthly['zri m2'] = monthly.groupby(['zip', 'Home Type'])['zri'].shift(2)
    monthly['zri m3'] = monthly.groupby(['zip', 'Home Type'])['zri'].shift(3)
    monthly['zri m4'] = monthly.groupby(['zip', 'Home Type'])['zri'].shift(4)
    monthly['zri y1'] = monthly.groupby(['zip', 'Home Type'])['zri'].shift(12)
    monthly['cap_rate m1'] = monthly.groupby(['zip', 'Home Type'])['cap_rate'].shift(1)
    monthly['cap_rate m2'] = monthly.groupby(['zip', 'Home Type'])['cap_rate'].shift(2)
    monthly['cap_rate m3'] = monthly.groupby(['zip', 'Home Type'])['cap_rate'].shift(3)
    monthly['cap_rate m4'] = monthly.groupby(['zip', 'Home Type'])['cap_rate'].shift(4)
    monthly['cap_rate y1'] = monthly.groupby(['zip', 'Home Type'])['cap_rate'].shift(12)
    monthly['zhvi_month_chg m1'] = monthly.groupby(['zip', 'Home Type'])['zhvi_month_pchg'].shift(1)
    monthly['zhvi_month_chg m2'] = monthly.groupby(['zip', 'Home Type'])['zhvi_month_pchg'].shift(2)
    monthly['zhvi_month_chg m3'] = monthly.groupby(['zip', 'Home Type'])['zhvi_month_pchg'].shift(3)
    monthly['zhvi_month_chg m4'] = monthly.groupby(['zip', 'Home Type'])['zhvi_month_pchg'].shift(4)
    monthly['zhvi_month_chg y1'] = monthly.groupby(['zip', 'Home Type'])['zhvi_month_pchg'].shift(12)
    monthly['zri_month_chg m1'] = monthly.groupby(['zip', 'Home Type'])['zri_month_pchg'].shift(1)
    monthly['zri_month_chg m2'] = monthly.groupby(['zip', 'Home Type'])['zri_month_pchg'].shift(2)
    monthly['zri_month_chg m3'] = monthly.groupby(['zip', 'Home Type'])['zri_month_pchg'].shift(3)
    monthly['zri_month_chg m4'] = monthly.groupby(['zip', 'Home Type'])['zri_month_pchg'].shift(4)
    monthly['zri_month_chg y1'] = monthly.groupby(['zip', 'Home Type'])['zri_month_pchg'].shift(12)
    monthly['cap_rate_month_chg m1'] = monthly.groupby(['zip', 'Home Type'])['cap_rate_month_pchg'].shift(1)
    monthly['cap_rate_month_chg m2'] = monthly.groupby(['zip', 'Home Type'])['cap_rate_month_pchg'].shift(2)
    monthly['cap_rate_month_chg m3'] = monthly.groupby(['zip', 'Home Type'])['cap_rate_month_pchg'].shift(3)
    monthly['cap_rate_month_chg m4'] = monthly.groupby(['zip', 'Home Type'])['cap_rate_month_pchg'].shift(4)
    monthly['cap_rate_month_chg y1'] = monthly.groupby(['zip', 'Home Type'])['cap_rate_month_pchg'].shift(12)


    monthly = monthly.merge(fed_rate[['Fed Rate', 'Fed Rate pchg']].add_suffix(' m0'),
                            how = 'left', left_on = 'index', right_index = True)
    monthly = monthly.merge(fed_rate[['Fed Rate', 'Fed Rate pchg', 'date m1']].add_suffix(' m1'),
                            how = 'left', left_on = 'index', right_on = 'date m1 m1').drop('date m1 m1', axis = 1)
    monthly = monthly.merge(fed_rate[['Fed Rate', 'Fed Rate pchg', 'date m2']].add_suffix(' m2'),
                            how = 'left', left_on = 'index', right_on = 'date m2 m2').drop('date m2 m2', axis = 1)
    monthly = monthly.merge(fed_rate[['Fed Rate', 'Fed Rate pchg', 'date m3']].add_suffix(' m3'),
                            how = 'left', left_on = 'index', right_on = 'date m3 m3').drop('date m3 m3', axis = 1)
    monthly = monthly.merge(fed_rate[['Fed Rate', 'Fed Rate pchg', 'date y1']].add_suffix(' y1'),
                            how = 'left', left_on = 'index', right_on = 'date y1 y1').drop('date y1 y1', axis = 1)

    monthly = monthly.merge(mortgage_rate[['30 FRM Rate', '30 FRM Rate pchg']].add_suffix(' m0'),
                            how = 'left', left_on = 'index', right_index = True)
    monthly = monthly.merge(mortgage_rate[['30 FRM Rate', '30 FRM Rate pchg', 'date m1']].add_suffix(' m1'),
                            how = 'left', left_on = 'index', right_on = 'date m1 m1').drop('date m1 m1', axis = 1)
    monthly = monthly.merge(mortgage_rate[['30 FRM Rate', '30 FRM Rate pchg', 'date m2']].add_suffix(' m2'),
                            how = 'left', left_on = 'index', right_on = 'date m2 m2').drop('date m2 m2', axis = 1)
    monthly = monthly.merge(mortgage_rate[['30 FRM Rate', '30 FRM Rate pchg', 'date m3']].add_suffix(' m3'),
                            how = 'left', left_on = 'index', right_on = 'date m3 m3').drop('date m3 m3', axis = 1)
    monthly = monthly.merge(mortgage_rate[['30 FRM Rate', '30 FRM Rate pchg', 'date y1']].add_suffix(' y1'),
                            how = 'left', left_on = 'index', right_on = 'date y1 y1').drop('date y1 y1', axis = 1)

    monthly = monthly.merge(market[['DOW avg', 'NASDAQ avg', 'SP500 avg', 'REZ avg', 'IYR avg', 'VNQ avg', 'SCHH avg', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg', 'REZ pchg', 'IYR pchg', 'VNQ pchg', 'SCHH pchg']].add_suffix(' m0'),
                            how = 'left', left_on = 'index', right_index = True)
    monthly = monthly.merge(market[['DOW avg', 'NASDAQ avg', 'SP500 avg', 'REZ avg', 'IYR avg', 'VNQ avg', 'SCHH avg', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg', 'REZ pchg', 'IYR pchg', 'VNQ pchg', 'SCHH pchg', 'date m1']].add_suffix(' m1'),
                            how = 'left', left_on = 'index', right_on = 'date m1 m1').drop('date m1 m1', axis = 1)
    monthly = monthly.merge(market[['DOW avg', 'NASDAQ avg', 'SP500 avg', 'REZ avg', 'IYR avg', 'VNQ avg', 'SCHH avg', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg', 'REZ pchg', 'IYR pchg', 'VNQ pchg', 'SCHH pchg', 'date m2']].add_suffix(' m2'),
                            how = 'left', left_on = 'index', right_on = 'date m2 m2').drop('date m2 m2', axis = 1)
    monthly = monthly.merge(market[['DOW avg', 'NASDAQ avg', 'SP500 avg', 'REZ avg', 'IYR avg', 'VNQ avg', 'SCHH avg', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg', 'REZ pchg', 'IYR pchg', 'VNQ pchg', 'SCHH pchg', 'date m3']].add_suffix(' m3'),
                            how = 'left', left_on = 'index', right_on = 'date m3 m3').drop('date m3 m3', axis = 1)
    monthly = monthly.merge(market[['DOW avg', 'NASDAQ avg', 'SP500 avg', 'REZ avg', 'IYR avg', 'VNQ avg', 'SCHH avg', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg', 'REZ pchg', 'IYR pchg', 'VNQ pchg', 'SCHH pchg', 'date y1']].add_suffix(' y1'),
                            how = 'left', left_on = 'index', right_on = 'date y1 y1').drop('date y1 y1', axis = 1)


    monthly_nonan = monthly[['zhvi', 'zri', 'cap_rate', 'zhvi_log', 'zri_log', 'cap_rate_log',
                             'zhvi_month_pchg','zri_month_pchg','cap_rate_month_pchg',
                             'zhvi m1','zhvi m2','zhvi m3','zhvi m4','zhvi y1',
                             'zri m1','zri m2','zri m3','zri m4','zri y1',
                             'cap_rate m1','cap_rate m2','cap_rate m3','cap_rate m4','cap_rate y1',
                             'zhvi_month_chg m1','zhvi_month_chg m2','zhvi_month_chg m3','zhvi_month_chg m4','zhvi_month_chg y1',
                             'zri_month_chg m1','zri_month_chg m2','zri_month_chg m3','zri_month_chg m4','zri_month_chg y1',
                             'cap_rate_month_chg m1','cap_rate_month_chg m2','cap_rate_month_chg m3','cap_rate_month_chg m4','cap_rate_month_chg y1',
                             
                             'Fed Rate m0', 'Fed Rate m1', 'Fed Rate m2', 'Fed Rate m3', 'Fed Rate y1', 
                             'Fed Rate pchg m0', 'Fed Rate pchg m1', 'Fed Rate pchg m2', 'Fed Rate pchg m3', 'Fed Rate pchg y1',
                             
                             '30 FRM Rate m0', '30 FRM Rate m1', '30 FRM Rate m2', '30 FRM Rate m3', '30 FRM Rate y1',
                             '30 FRM Rate pchg m0', '30 FRM Rate pchg m1', '30 FRM Rate pchg m2', '30 FRM Rate pchg m3', '30 FRM Rate pchg y1',
                             
                             'DOW avg m0', 'DOW avg m1', 'DOW avg m2', 'DOW avg m3', 'DOW avg y1', 
                             'DOW pchg m0', 'DOW pchg m1', 'DOW pchg m2', 'DOW pchg m3', 'DOW pchg y1',
                             'NASDAQ avg m0', 'NASDAQ avg m1', 'NASDAQ avg m2', 'NASDAQ avg m3','NASDAQ avg y1',
                             'NASDAQ pchg m0', 'NASDAQ pchg m1', 'NASDAQ pchg m2', 'NASDAQ pchg m3', 'NASDAQ pchg y1', 
                             'SP500 avg m0', 'SP500 avg m1', 'SP500 avg m2', 'SP500 avg m3', 'SP500 avg y1', 
                             'SP500 pchg m0', 'SP500 pchg m1', 'SP500 pchg m2', 'SP500 pchg m3', 'SP500 pchg y1',
                             'REZ avg m0', 'REZ avg m1', 'REZ avg m2', 'REZ avg m3', 'REZ avg y1', 
                             'REZ pchg m0', 'REZ pchg m1', 'REZ pchg m2', 'REZ pchg m3', 'REZ pchg y1',
                             'IYR avg m0', 'IYR avg m1', 'IYR avg m2', 'IYR avg m3', 'IYR avg y1', 
                             'IYR pchg m0', 'IYR pchg m1', 'IYR pchg m2', 'IYR pchg m3', 'IYR pchg y1', 
                             'VNQ avg m0', 'VNQ avg m1', 'VNQ avg m2', 'VNQ avg m3', 'VNQ avg y1', 
                             'VNQ pchg m0', 'VNQ pchg m1', 'VNQ pchg m2', 'VNQ pchg m3', 'VNQ pchg y1', 
                             'SCHH avg m0', 'SCHH avg m1', 'SCHH avg m2', 'SCHH avg m3','SCHH avg y1',
                             'SCHH pchg m0', 'SCHH pchg m1', 'SCHH pchg m2', 'SCHH pchg m3', 'SCHH pchg y1']].dropna(how = 'any')

    corr = np.corrcoef([monthly_nonan['zhvi'], monthly_nonan['zri'], monthly_nonan['cap_rate'],
                        monthly_nonan['zhvi_log'], monthly_nonan['zri_log'], monthly_nonan['cap_rate_log'],
                        monthly_nonan['zhvi_month_pchg'], monthly_nonan['zri_month_pchg'],monthly_nonan['cap_rate_month_pchg'],
                        monthly_nonan['zhvi m1'], monthly_nonan['zhvi m2'], monthly_nonan['zhvi m3'], monthly_nonan['zhvi m4'], monthly_nonan['zhvi y1'],
                        monthly_nonan['zri m1'], monthly_nonan['zri m2'], monthly_nonan['zri m3'], monthly_nonan['zri m4'], monthly_nonan['zri y1'],
                        monthly_nonan['cap_rate m1'], monthly_nonan['cap_rate m2'], monthly_nonan['cap_rate m3'],monthly_nonan['cap_rate m4'], monthly_nonan['cap_rate y1'],
                        monthly_nonan['zhvi_month_chg m1'], monthly_nonan['zhvi_month_chg m2'], monthly_nonan['zhvi_month_chg m3'], monthly_nonan['zhvi_month_chg m4'], monthly_nonan['zhvi_month_chg y1'],
                        monthly_nonan['zri_month_chg m1'], monthly_nonan['zri_month_chg m2'], monthly_nonan['zri_month_chg m3'], monthly_nonan['zri_month_chg m4'], monthly_nonan['zri_month_chg y1'],
                        monthly_nonan['cap_rate_month_chg m1'], monthly_nonan['cap_rate_month_chg m2'], monthly_nonan['cap_rate_month_chg m3'], monthly_nonan['cap_rate_month_chg m4'], monthly_nonan['cap_rate_month_chg y1'],
                        monthly_nonan['Fed Rate m0'], monthly_nonan['Fed Rate m1'], monthly_nonan['Fed Rate m2'], monthly_nonan['Fed Rate m3'], monthly_nonan['Fed Rate y1'], 
                        monthly_nonan['Fed Rate pchg m0'], monthly_nonan['Fed Rate pchg m1'], monthly_nonan['Fed Rate pchg m2'], monthly_nonan['Fed Rate pchg m3'], monthly_nonan['Fed Rate pchg y1'],
                        monthly_nonan['30 FRM Rate m0'], monthly_nonan['30 FRM Rate m1'], monthly_nonan['30 FRM Rate m2'], monthly_nonan['30 FRM Rate m3'], monthly_nonan['30 FRM Rate y1'], 
                        monthly_nonan['30 FRM Rate pchg m0'], monthly_nonan['30 FRM Rate pchg m1'], monthly_nonan['30 FRM Rate pchg m2'], monthly_nonan['30 FRM Rate pchg m3'], monthly_nonan['30 FRM Rate pchg y1'],
                        monthly_nonan['DOW avg m0'], monthly_nonan['DOW avg m1'], monthly_nonan['DOW avg m2'], monthly_nonan['DOW avg m3'], monthly_nonan['DOW avg y1'],
                        monthly_nonan['DOW pchg m0'], monthly_nonan['DOW pchg m1'], monthly_nonan['DOW pchg m2'], monthly_nonan['DOW pchg m3'], monthly_nonan['DOW pchg y1'],
                        monthly_nonan['NASDAQ avg m0'],monthly_nonan['NASDAQ avg m1'], monthly_nonan['NASDAQ avg m2'], monthly_nonan['NASDAQ avg m3'], monthly_nonan['NASDAQ avg y1'],
                        monthly_nonan['NASDAQ pchg m0'], monthly_nonan['NASDAQ pchg m1'], monthly_nonan['NASDAQ pchg m2'], monthly_nonan['NASDAQ pchg m3'], monthly_nonan['NASDAQ pchg y1'],
                        monthly_nonan['SP500 avg m0'], monthly_nonan['SP500 avg m1'], monthly_nonan['SP500 avg m2'], monthly_nonan['SP500 avg m3'], monthly_nonan['SP500 avg y1'], 
                        monthly_nonan['SP500 pchg m0'], monthly_nonan['SP500 pchg m1'], monthly_nonan['SP500 pchg m2'], monthly_nonan['SP500 pchg m3'], monthly_nonan['SP500 pchg y1'],
                        monthly_nonan['REZ avg m0'], monthly_nonan['REZ avg m1'], monthly_nonan['REZ avg m2'], monthly_nonan['REZ avg m3'], monthly_nonan['REZ avg y1'],
                        monthly_nonan['REZ pchg m0'], monthly_nonan['REZ pchg m1'], monthly_nonan['REZ pchg m2'], monthly_nonan['REZ pchg m3'], monthly_nonan['REZ pchg y1'],
                        monthly_nonan['IYR avg m0'], monthly_nonan['IYR avg m1'], monthly_nonan['IYR avg m2'], monthly_nonan['IYR avg m3'], monthly_nonan['IYR avg y1'],
                        monthly_nonan['IYR pchg m0'], monthly_nonan['IYR pchg m1'], monthly_nonan['IYR pchg m2'], monthly_nonan['IYR pchg m3'], monthly_nonan['IYR pchg y1'],
                        monthly_nonan['VNQ avg m0'], monthly_nonan['VNQ avg m1'], monthly_nonan['VNQ avg m2'], monthly_nonan['VNQ avg m3'],monthly_nonan['VNQ avg y1'],
                        monthly_nonan['VNQ pchg m0'], monthly_nonan['VNQ pchg m1'], monthly_nonan['VNQ pchg m2'], monthly_nonan['VNQ pchg m3'], monthly_nonan['VNQ pchg y1'],
                        monthly_nonan['SCHH avg m0'], monthly_nonan['SCHH avg m1'], monthly_nonan['SCHH avg m2'], monthly_nonan['SCHH avg m3'], monthly_nonan['SCHH avg y1'], 
                        monthly_nonan['SCHH pchg m0'], monthly_nonan['SCHH pchg m1'], monthly_nonan['SCHH pchg m2'], monthly_nonan['SCHH pchg m3'], monthly_nonan['SCHH pchg y1']])


    pd.DataFrame(corr, columns = ['zhvi','zri','cap_rate','zhvi_log','zri_log','cap_rate_log',
                                  'zhvi_month_pchg','zri_month_pchg','cap_rate_month_pchg',
                                  'zhvi m1','zhvi m2','zhvi m3','zhvi m4','zhvi y1',
                                  'zri m1','zri m2','zri m3','zri m4','zri y1',
                                  'cap_rate m1','cap_rate m2','cap_rate m3','cap_rate m4','cap_rate y1',
                                  'zhvi_month_chg m1','zhvi_month_chg m2','zhvi_month_chg m3','zhvi_month_chg m4','zhvi_month_chg y1',
                                  'zri_month_chg m1','zri_month_chg m2','zri_month_chg m3','zri_month_chg m4','zri_month_chg y1',
                                  'cap_rate_month_chg m1','cap_rate_month_chg m2','cap_rate_month_chg m3','cap_rate_month_chg m4','cap_rate_month_chg y1',
                                  'Fed Rate m0','Fed Rate m1','Fed Rate m2','Fed Rate m3','Fed Rate y1',
                                  'Fed Rate pchg m0','Fed Rate pchg m1','Fed Rate pchg m2','Fed Rate pchg m3','Fed Rate pchg y1',
                                  '30 FRM Rate m0','30 FRM Rate m1','30 FRM Rate m2','30 FRM Rate m3','30 FRM Rate y1',
                                  '30 FRM Rate pchg m0','30 FRM Rate pchg m1','30 FRM Rate pchg m2','30 FRM Rate pchg m3','30 FRM Rate pchg y1',
                                  'DOW avg m0','DOW avg m1','DOW avg m2','DOW avg m3','DOW avg y1',
                                  'DOW pchg m0','DOW pchg m1','DOW pchg m2','DOW pchg m3','DOW pchg y1',
                                  'NASDAQ avg m0','NASDAQ avg m1','NASDAQ avg m2','NASDAQ avg m3','NASDAQ avg y1',
                                  'NASDAQ pchg m0','NASDAQ pchg m1','NASDAQ pchg m2','NASDAQ pchg m3','NASDAQ pchg y1',
                                  'SP500 avg m0','SP500 avg m1','SP500 avg m2','SP500 avg m3','SP500 avg y1',
                                  'SP500 pchg m0','SP500 pchg m1','SP500 pchg m2','SP500 pchg m3','SP500 pchg y1',
                                  'REZ avg m0','REZ avg m1','REZ avg m2','REZ avg m3','REZ avg y1',
                                  'REZ pchg m0','REZ pchg m1','REZ pchg m2','REZ pchg m3','REZ pchg y1',
                                  'IYR avg m0','IYR avg m1','IYR avg m2','IYR avg m3','IYR avg y1',
                                  'IYR pchg m0','IYR pchg m1','IYR pchg m2','IYR pchg m3','IYR pchg y1',
                                  'VNQ avg m0','VNQ avg m1','VNQ avg m2','VNQ avg m3','VNQ avg y1',
                                  'VNQ pchg m0','VNQ pchg m1','VNQ pchg m2','VNQ pchg m3','VNQ pchg y1',
                                  'SCHH avg m0','SCHH avg m1','SCHH avg m2','SCHH avg m3','SCHH avg y1',
                                  'SCHH pchg m0','SCHH pchg m1','SCHH pchg m2','SCHH pchg m3','SCHH pchg y1'],
                 index = ['zhvi','zri','cap_rate','zhvi_log','zri_log','cap_rate_log',
                          'zhvi_month_pchg','zri_month_pchg','cap_rate_month_pchg',
                          'zhvi m1','zhvi m2','zhvi m3','zhvi m4','zhvi y1',
                          'zri m1','zri m2','zri m3','zri m4','zri y1',
                          'cap_rate m1','cap_rate m2','cap_rate m3','cap_rate m4','cap_rate y1',
                          'zhvi_month_chg m1','zhvi_month_chg m2','zhvi_month_chg m3','zhvi_month_chg m4','zhvi_month_chg y1',
                          'zri_month_chg m1','zri_month_chg m2','zri_month_chg m3','zri_month_chg m4','zri_month_chg y1',
                          'cap_rate_month_chg m1','cap_rate_month_chg m2','cap_rate_month_chg m3','cap_rate_month_chg m4','cap_rate_month_chg y1',
                          'Fed Rate m0','Fed Rate m1','Fed Rate m2','Fed Rate m3','Fed Rate y1',
                          'Fed Rate pchg m0','Fed Rate pchg m1','Fed Rate pchg m2','Fed Rate pchg m3','Fed Rate pchg y1',
                          '30 FRM Rate m0','30 FRM Rate m1','30 FRM Rate m2','30 FRM Rate m3','30 FRM Rate y1',
                          '30 FRM Rate pchg m0','30 FRM Rate pchg m1','30 FRM Rate pchg m2','30 FRM Rate pchg m3','30 FRM Rate pchg y1',
                          'DOW avg m0','DOW avg m1','DOW avg m2','DOW avg m3','DOW avg y1',
                          'DOW pchg m0','DOW pchg m1','DOW pchg m2','DOW pchg m3','DOW pchg y1',
                          'NASDAQ avg m0','NASDAQ avg m1','NASDAQ avg m2','NASDAQ avg m3','NASDAQ avg y1',
                          'NASDAQ pchg m0','NASDAQ pchg m1','NASDAQ pchg m2','NASDAQ pchg m3','NASDAQ pchg y1',
                          'SP500 avg m0','SP500 avg m1','SP500 avg m2','SP500 avg m3','SP500 avg y1',
                          'SP500 pchg m0','SP500 pchg m1','SP500 pchg m2','SP500 pchg m3','SP500 pchg y1',
                          'REZ avg m0','REZ avg m1','REZ avg m2','REZ avg m3','REZ avg y1',
                          'REZ pchg m0','REZ pchg m1','REZ pchg m2','REZ pchg m3','REZ pchg y1',
                          'IYR avg m0','IYR avg m1','IYR avg m2','IYR avg m3','IYR avg y1',
                          'IYR pchg m0','IYR pchg m1','IYR pchg m2','IYR pchg m3','IYR pchg y1',
                          'VNQ avg m0','VNQ avg m1','VNQ avg m2','VNQ avg m3','VNQ avg y1',
                          'VNQ pchg m0','VNQ pchg m1','VNQ pchg m2','VNQ pchg m3','VNQ pchg y1',
                          'SCHH avg m0','SCHH avg m1','SCHH avg m2','SCHH avg m3','SCHH avg y1',
                          'SCHH pchg m0','SCHH pchg m1','SCHH pchg m2','SCHH pchg m3','SCHH pchg y1']).to_csv('monthly_corr_matrix.csv')


    if False:   
        for idy, yname in enumerate(['zhvi', 'zri', 'cap_rate', 'zhvi_log', 'zri_log', 'cap_rate_log', 'zhvi_month_pchg', 'zri_month_pchg', 'cap_rate_month_pchg']):
            for idx, xname in enumerate(monthly_nonan.columns.values[9:]):
                sns.jointplot(data = monthly_nonan, x = xname, y = yname, size=7, kind = 'reg')
                plt.tight_layout()
                plt.show()
    
    

        combined = combined.merge(market_annual[['year_p1', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg',
                                                 'DOW perf', 'NASDAQ perf', 'SP500 perf']].add_suffix(' p1'),
                                  how = 'left', left_on = 'year', right_on = 'year_p1 p1').drop('year_p1 p1', axis = 1)
        combined = combined.merge(market_annual[['year_p2', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg',
                                                 'DOW perf', 'NASDAQ perf', 'SP500 perf']].add_suffix(' p2'),
                                  how = 'left', left_on = 'year', right_on = 'year_p2 p2').drop('year_p2 p2', axis = 1)
        combined = combined.merge(market_annual[['year_p3', 'DOW pchg', 'NASDAQ pchg', 'SP500 pchg',
                                                 'DOW perf', 'NASDAQ perf', 'SP500 perf']].add_suffix(' p3'),
                                  how = 'left', left_on = 'year', right_on = 'year_p3 p3').drop('year_p3 p3', axis = 1)
        
    




        combined_nonan = combined[['zhvi', 'zri', 'cap_rate', 'zhvi_annual_chg', 'zri_annual_chg',
       'cap_rate_annual_chg', 'zhvi_p1', 'zri_p1', 'cap_rate_p1', 'zhvi_annual_chg_p1',
       'zri_annual_chg_p1', 'cap_rate_annual_chg_p1', 
       'num R p3', 'avg AGI p3', 'avg SW p3', 'avg OI p3',
       'avg AGI pchg p3', 'avg SW pchg p3', 'avg OI pchg p3',
       'num R pchg p3', 'num R p2', 'avg AGI p2', 'avg SW p2', 'avg OI p2',
       'avg AGI pchg p2', 'avg SW pchg p2', 'avg OI pchg p2',
       'num R pchg p2', 'Fed Rate p1', 'Fed Rate pchg p1', 'Fed Rate p2',
       'Fed Rate pchg p2', 'Fed Rate p3', 'Fed Rate pchg p3', '30 FRM Rate p1',
       '30 FRM Rate chg p1', '30 FRM Rate p2', '30 FRM Rate chg p2',
       '30 FRM Rate p3', '30 FRM Rate chg p3',
       'DOW pchg p1', 'NASDAQ pchg p1', 'SP500 pchg p1', 'DOW perf p1',
       'NASDAQ perf p1', 'SP500 perf p1', 'DOW pchg p2', 'NASDAQ pchg p2',
       'SP500 pchg p2', 'DOW perf p2', 'NASDAQ perf p2', 'SP500 perf p2',
       'DOW pchg p3', 'NASDAQ pchg p3', 'SP500 pchg p3', 'DOW perf p3',
       'NASDAQ perf p3', 'SP500 perf p3']].dropna(how = 'any')
        '''
        combined[(~np.isnan(combined['zhvi'])) &
                                  (~np.isnan(combined['zri'])) &
                                  (~np.isnan(combined['cap_rate'])) &
                                  (~np.isnan(combined['zhvi_annual_chg'])) &
                                  (~np.isnan(combined['zri_annual_chg'])) &
                                  (~np.isnan(combined['cap_rate_annual_chg'])) &
                                  (~np.isnan(combined['zhvi_p1'])) &
                                  (~np.isnan(combined['zri_p1'])) &
                                  (~np.isnan(combined['cap_rate_p1'])) &
                                  (~np.isnan(combined['zhvi_annual_chg_p1'])) &
                                  (~np.isnan(combined['zri_annual_chg_p1'])) &
                                  (~np.isnan(combined['cap_rate_annual_chg_p1'])) &
                                  (~np.isnan(combined['num R'])) &
                                  (~np.isnan(combined['num R pchg'])) &
                                  (~np.isnan(combined['avg AGI'])) &
                                  (~np.isnan(combined['avg AGI pchg'])) &
                                  (~np.isnan(combined['avg SW'])) &
                                  (~np.isnan(combined['avg SW pchg'])) &
                                  (~np.isnan(combined['avg OI'])) &
                                  (~np.isnan(combined['avg OI pchg'])) &
                                  (~np.isnan(combined['Fed Rate'])) &
                                  (~np.isnan(combined['Fed Rate pchg'])) &
                                  (~np.isnan(combined['30 FRM Rate'])) &
                                  (~np.isnan(combined['30 FRM Rate chg'])) &
                                  (~np.isnan(combined['DOW pchg'])) &
                                  (~np.isnan(combined['NASDAQ pchg'])) &
                                  (~np.isnan(combined['SP500 pchg'])) &
                                  (~np.isnan(combined['DOW perf'])) &
                                  (~np.isnan(combined['NASDAQ perf'])) &
                                  (~np.isnan(combined['SP500 perf']))]
        '''
        print(combined_nonan.shape) #(2516, 58)
        
        combined_nonan['zhvi_log'] = np.log(combined_nonan['zhvi'])
        combined_nonan['zri_log'] = np.log(combined_nonan['zri'])
        combined_nonan['cap_rate_log'] = np.log(combined_nonan['cap_rate'])
        #combined_nonan['num R c'] = round((combined_nonan['num R'] - combined_nonan['num R'].mean()) / combined_nonan['num R'].std())

        #let's look at some cap rate correlations

        
        corr = np.corrcoef([combined_nonan['zhvi'],
                            combined_nonan['zhvi_log'],
                            combined_nonan['zri'],
                            combined_nonan['zri_log'],
                            combined_nonan['cap_rate'],
                            combined_nonan['cap_rate_log'],
                            combined_nonan['zhvi_annual_chg'],
                            combined_nonan['zri_annual_chg'],
                            combined_nonan['cap_rate_annual_chg'],
                            combined_nonan['zhvi_p1'],
                            combined_nonan['zri_p1'],
                            combined_nonan['cap_rate_p1'],
                            combined_nonan['zhvi_annual_chg_p1'],
                            combined_nonan['zri_annual_chg_p1'],
                            combined_nonan['cap_rate_annual_chg_p1'],
                            combined_nonan['num R p3'],
                            combined_nonan['avg AGI p3'],
                            combined_nonan['avg SW p3'],
                            combined_nonan['avg OI p3'],
                            combined_nonan['num R pchg p3'],
                            combined_nonan['avg AGI pchg p3'],
                            combined_nonan['avg SW pchg p3'],
                            combined_nonan['avg OI pchg p3'],
                            combined_nonan['num R p2'],
                            combined_nonan['avg AGI p2'],
                            combined_nonan['avg SW p2'],
                            combined_nonan['avg OI p2'],
                            combined_nonan['num R pchg p2'],
                            combined_nonan['avg AGI pchg p2'],
                            combined_nonan['avg SW pchg p2'],
                            combined_nonan['avg OI pchg p2'],
                            combined_nonan['Fed Rate p1'],
                            combined_nonan['Fed Rate pchg p1'],
                            combined_nonan['Fed Rate p2'],
                            combined_nonan['Fed Rate pchg p2'],
                            combined_nonan['Fed Rate p3'],
                            combined_nonan['Fed Rate pchg p3'],
                            combined_nonan['30 FRM Rate p1'],
                            combined_nonan['30 FRM Rate chg p1'],
                            combined_nonan['30 FRM Rate p2'],
                            combined_nonan['30 FRM Rate chg p2'],
                            combined_nonan['30 FRM Rate p3'],
                            combined_nonan['30 FRM Rate chg p3'],
                            combined_nonan['DOW pchg p1'],
                            combined_nonan['NASDAQ pchg p1'],
                            combined_nonan['SP500 pchg p1'],
                            combined_nonan['DOW perf p1'],
                            combined_nonan['NASDAQ perf p1'],
                            combined_nonan['SP500 perf p1'],
                            combined_nonan['DOW pchg p2'],
                            combined_nonan['NASDAQ pchg p2'],
                            combined_nonan['SP500 pchg p2'],
                            combined_nonan['DOW perf p2'],
                            combined_nonan['NASDAQ perf p2'],
                            combined_nonan['SP500 perf p2'],
                            combined_nonan['DOW pchg p3'],
                            combined_nonan['NASDAQ pchg p3'],
                            combined_nonan['SP500 pchg p3'],
                            combined_nonan['DOW perf p3'],
                            combined_nonan['NASDAQ perf p3'],
                            combined_nonan['SP500 perf p3']])

        '''
['zhvi','zhvi_log','zri','zri_log','cap_rate','cap_rate_log','zhvi_annual_chg','zri_annual_chg',
'cap_rate_annual_chg','zhvi_p1','zri_p1','cap_rate_p1','zhvi_annual_chg_p1','zri_annual_chg_p1',
'cap_rate_annual_chg_p1','num R p3','avg AGI p3','avg SW p3','avg OI p3','num R pchg p3',
'avg AGI pchg p3','avg SW pchg p3','avg OI pchg p3','num R p2','avg AGI p2','avg SW p2',
'avg OI p2','num R pchg p2','avg AGI pchg p2','avg SW pchg p2','avg OI pchg p2','Fed Rate p1',
'Fed Rate pchg p1','Fed Rate p2','Fed Rate pchg p2','Fed Rate p3','Fed Rate pchg p3',
'30 FRM Rate p1','30 FRM Rate chg p1','30 FRM Rate p2','30 FRM Rate chg p2','30 FRM Rate p3',
'30 FRM Rate chg p3','DOW pchg p1','NASDAQ pchg p1','SP500 pchg p1','DOW perf p1','NASDAQ perf p1',
'SP500 perf p1','DOW pchg p2','NASDAQ pchg p2','SP500 pchg p2','DOW perf p2','NASDAQ perf p2',
'SP500 perf p2','DOW pchg p3','NASDAQ pchg p3','SP500 pchg p3','DOW perf p3','NASDAQ perf p3',
'SP500 perf p3']    
        '''


        pd.DataFrame(corr, columns = ['zhvi','zhvi_log','zri','zri_log','cap_rate','cap_rate_log','zhvi_annual_chg','zri_annual_chg',
'cap_rate_annual_chg','zhvi_p1','zri_p1','cap_rate_p1','zhvi_annual_chg_p1','zri_annual_chg_p1',
'cap_rate_annual_chg_p1','num R p3','avg AGI p3','avg SW p3','avg OI p3','num R pchg p3',
'avg AGI pchg p3','avg SW pchg p3','avg OI pchg p3','num R p2','avg AGI p2','avg SW p2',
'avg OI p2','num R pchg p2','avg AGI pchg p2','avg SW pchg p2','avg OI pchg p2','Fed Rate p1',
'Fed Rate pchg p1','Fed Rate p2','Fed Rate pchg p2','Fed Rate p3','Fed Rate pchg p3',
'30 FRM Rate p1','30 FRM Rate chg p1','30 FRM Rate p2','30 FRM Rate chg p2','30 FRM Rate p3',
'30 FRM Rate chg p3','DOW pchg p1','NASDAQ pchg p1','SP500 pchg p1','DOW perf p1','NASDAQ perf p1',
'SP500 perf p1','DOW pchg p2','NASDAQ pchg p2','SP500 pchg p2','DOW perf p2','NASDAQ perf p2',
'SP500 perf p2','DOW pchg p3','NASDAQ pchg p3','SP500 pchg p3','DOW perf p3','NASDAQ perf p3',
'SP500 perf p3'] ,
                     index = ['zhvi','zhvi_log','zri','zri_log','cap_rate','cap_rate_log','zhvi_annual_chg','zri_annual_chg',
'cap_rate_annual_chg','zhvi_p1','zri_p1','cap_rate_p1','zhvi_annual_chg_p1','zri_annual_chg_p1',
'cap_rate_annual_chg_p1','num R p3','avg AGI p3','avg SW p3','avg OI p3','num R pchg p3',
'avg AGI pchg p3','avg SW pchg p3','avg OI pchg p3','num R p2','avg AGI p2','avg SW p2',
'avg OI p2','num R pchg p2','avg AGI pchg p2','avg SW pchg p2','avg OI pchg p2','Fed Rate p1',
'Fed Rate pchg p1','Fed Rate p2','Fed Rate pchg p2','Fed Rate p3','Fed Rate pchg p3',
'30 FRM Rate p1','30 FRM Rate chg p1','30 FRM Rate p2','30 FRM Rate chg p2','30 FRM Rate p3',
'30 FRM Rate chg p3','DOW pchg p1','NASDAQ pchg p1','SP500 pchg p1','DOW perf p1','NASDAQ perf p1',
'SP500 perf p1','DOW pchg p2','NASDAQ pchg p2','SP500 pchg p2','DOW perf p2','NASDAQ perf p2',
'SP500 perf p2','DOW pchg p3','NASDAQ pchg p3','SP500 pchg p3','DOW perf p3','NASDAQ perf p3',
'SP500 perf p3']).to_csv('corr_matrix.csv')
        
    if False:
        if False:
            #let's first look at cap rate. we should look into these 10 zip codes to see what's happening
            #last 5 grow in cap rate: 
            temp1 = temp[(~np.isnan(temp['cap_rate'])) & (temp['Home Type'] == 'All Homes')].groupby(['zip', 'Home Type'])['cap_rate'].last()/temp[(~np.isnan(temp['cap_rate'])) & (temp['Home Type'] == 'All Homes')].groupby(['zip', 'Home Type'])['cap_rate'].first() - 1
            print(temp1.reset_index().sort_values(by = 'cap_rate').head(5))
            '''
                   zip  Home Type  cap_rate
            548  14303  All Homes -0.577723
            547  14301  All Homes -0.570744
            172  11221  All Homes -0.425415
            550  14305  All Homes -0.418025
            544  14138  All Homes -0.411318
            '''
            #top 5 grow in cap rate:
            print(temp1.reset_index().sort_values(by = 'cap_rate').tail(5))
            '''
                   zip  Home Type  cap_rate
            612  14626  All Homes  0.447303
            566  14472  All Homes  0.460159
            589  14580  All Homes  0.461170
            579  14526  All Homes  0.484366
            611  14625  All Homes  0.492515
            '''
            temp[(temp['zip'] == '14303') | (temp['zip'] == '14301') |
                 (temp['zip'] == '11221') | (temp['zip'] == '14305') |
                 (temp['zip'] == '14138') | (temp['zip'] == '14626') |
                 (temp['zip'] == '14472') | (temp['zip'] == '14580') |
                 (temp['zip'] == '14526') | (temp['zip'] == '14625')].to_csv('some_zips.csv', index = False)


        
        
        df = combined_nonan[['zhvi','zhvi_log','zri','zri_log','cap_rate','cap_rate_log','zhvi_annual_chg','zri_annual_chg',
'cap_rate_annual_chg','zhvi_p1','zri_p1','cap_rate_p1','zhvi_annual_chg_p1','zri_annual_chg_p1',
'cap_rate_annual_chg_p1','num R p3','avg AGI p3','avg SW p3','avg OI p3','num R pchg p3',
'avg AGI pchg p3','avg SW pchg p3','avg OI pchg p3','num R p2','avg AGI p2','avg SW p2',
'avg OI p2','num R pchg p2','avg AGI pchg p2','avg SW pchg p2','avg OI pchg p2','Fed Rate p1',
'Fed Rate pchg p1','Fed Rate p2','Fed Rate pchg p2','Fed Rate p3','Fed Rate pchg p3',
'30 FRM Rate p1','30 FRM Rate chg p1','30 FRM Rate p2','30 FRM Rate chg p2','30 FRM Rate p3',
'30 FRM Rate chg p3','DOW pchg p1','NASDAQ pchg p1','SP500 pchg p1','DOW perf p1','NASDAQ perf p1',
'SP500 perf p1','DOW pchg p2','NASDAQ pchg p2','SP500 pchg p2','DOW perf p2','NASDAQ perf p2',
'SP500 perf p2','DOW pchg p3','NASDAQ pchg p3','SP500 pchg p3','DOW perf p3','NASDAQ perf p3',
'SP500 perf p3']]


        for idy, yname in enumerate(df.columns.values[:9]):
            for idx, xname in enumerate(df.columns.values[9:]):
                sns.jointplot(data = df, x = xname, y = yname, size=7, kind = 'reg')
                plt.tight_layout()
                plt.show()


        #some annectotal stuff that is worth investigating
        #1. zhvi vs. zhvi_p1
        #2. zhvi vs. zri_p1
        #3. zhvi vs. cap_rate_p1
        #4. zhvi vs. avg AGI
        #5. zhvi vs. avg SW
        #6. 
        #7. 
        #8. 
        #9. 
        #10. 
        #11. 
        #12. 
        #13. 
        #14. 
        #15. 
        #16. 
                    
        combined_nonan = combined[(~np.isnan(combined['zhvi_annual_chg']))
                                  & (~np.isnan(combined['avg SW pchg']))]
        corr, pval = pearsonr(combined_nonan['zhvi_annual_chg'], combined_nonan['avg SW pchg'])
        sns.lmplot(data = combined_nonan, x = 'avg SW pchg', y = 'zhvi_annual_chg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #(0.18271720140029193, 3.5147389204204972e-30)
        plt.tight_layout()
        plt.show()

        #2 - num R pchg vs. zhvi annual chg
        corr, pval = pearsonr(combined_nonan['num R pchg'], combined_nonan['zhvi_annual_chg'])
        sns.lmplot(data = combined_nonan, x = 'num R pchg', y = 'zhvi_annual_chg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #corr: 0.0850725790075, p-val: 1.74824597919e-07
        plt.tight_layout()
        plt.show()
        print(corr, pval)

        #3 - cap_rate vs. zhvi annual chg #why is it that the higher the cap rate, the lower the change? could have something to do with population density
        corr, pval = pearsonr(combined_nonan['cap_rate'], combined_nonan['zhvi_annual_chg'])
        sns.lmplot(data = combined_nonan, x = 'cap_rate', y = 'zhvi_annual_chg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #corr: -0.42158681793706854, p-val: 7.6516131518351247e-134
        plt.tight_layout()
        plt.show()
        print(corr, pval)

        # if we remove the outliers, what would happen?
        #1 - avg SGI pchg vs. zhvi annual chg
        combined_nonan_3_std = combined_nonan[(np.abs(stats.zscore(combined_nonan[['avg AGI pchg','zhvi_annual_chg']])) < 3).all(axis = 1)]
        corr, pval = pearsonr(combined_nonan_3_std['avg AGI pchg'], combined_nonan_3_std['zhvi_annual_chg'])
        sns.lmplot(data = combined_nonan_3_std, x = 'avg AGI pchg', y = 'zhvi_annual_chg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #corr: 0.08960949601702384, p-val: 3.712725440009196e-08
        plt.tight_layout()
        plt.show()

        sns.jointplot(data = combined_nonan_3_std, x = 'avg AGI pchg', y = 'zhvi_annual_chg',
                      size=7, kind = 'kde', space = 0)
        plt.tight_layout()
        plt.show()

        #2 - num R pchg vs. zhvi annual chg
        corr, pval = pearsonr(combined_nonan_3_std['num R pchg'], combined_nonan_3_std['zhvi_annual_chg'])
        sns.lmplot(data = combined_nonan_3_std, x = 'num R pchg', y = 'zhvi_annual_chg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #corr: 0.0691293123349, p-val: 3.02694090867e-05
        plt.tight_layout()
        plt.show()
        print(corr, pval)

        #3 - avg SGI pchg vs.num R pchg
        corr, pval = pearsonr(combined_nonan_3_std['num R pchg'], combined_nonan_3_std['avg AGI pchg'])
        sns.lmplot(data = combined_nonan_3_std, x = 'num R pchg', y = 'avg AGI pchg', size=7, aspect=1.5, 
                   scatter_kws={'s': 10, 'alpha': 0.5, 'edgecolor':'k', 'linewidths': 1})
        plt.title('corr: {}, p-val: {}'.format(corr, pval)) #corr: 0.0691293123349, p-val: 3.02694090867e-05
        plt.tight_layout()
        plt.show()
        print(corr, pval)
'''
All Homes
272181.464778 185100.0 249137.558665 3136700.0 38300.0
38300.0 1019594.14077
Condo
413008.014933 322800.0 323958.872415 3121100.0 33800.0
33800.0 1384884.63218
Middle Tier
273389.749991 186000.0 250676.817239 3136700.0 38300.0
38300.0 1025420.20171
Bottom Tier
207892.809853 149160.0 166020.977513 1592000.0 31100.0
31100.0 705955.742392
One Bed
267734.686246 176100.0 245018.761354 1255300.0 36700.0
36700.0 1002790.97031
Top Tier
391324.490915 253400.0 451699.482048 6799100.0 42000.0
42000.0 1746422.93706
Two Bed
173218.57013 106000.0 223466.570528 3347100.0 32700.0
32700.0 843618.281714
Studio
485066.740826 419400.0 262244.130384 2724400.0 66000.0
66000.0 1271799.13198
Three Bed
233530.0542 156900.0 233314.365329 4942200.0 39100.0
39100.0 933473.150188
Four Bed
279637.65391 205600.0 221574.944079 1961800.0 41800.0
41800.0 944362.486146
Single Fam
270736.430785 183500.0 250194.444766 4549300.0 38300.0
38300.0 1021319.76508
Many Bed
462055.913567 306300.0 494432.613279 5848000.0 43600.0
43600.0 1945353.7534
'''
#temp.to_csv('test.csv', index = False)

'''

#Median Sold Price -
quandl_med_sold = pd.read_sql('select * from quandl_med_sold;', engine)
print(quandl_med_sold.shape) #(57252, 4)

#Median Sold Price Per Square Foot -
quandl_med_sold_sqft = pd.read_sql('select * from quandl_med_sold_sqft;', engine)
print(quandl_med_sold_sqft.shape) #(141662, 4)

#Median Value Per Square Foot -
quandl_med_val_sqft = pd.read_sql('select * from quandl_med_val_sqft;', engine)
print(quandl_med_val_sqft.shape) #(94877, 4)

#Median Rental Price Per Square -
quandl_med_rent_sqft = pd.read_sql('select * from quandl_med_rent_sqft;', engine)
print(quandl_med_rent_sqft.shape) #(7364, 4)

#Median Rental Price
quandl_rent_p = pd.read_sql('select * from quandl_rent_p;', engine)
print(quandl_rent_p.shape) #(21278, 4)
'''


