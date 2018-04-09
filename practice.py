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

def zillow_zipcode_pull():
    #this method updates the location zip codes only.
    #doesn't need to be run all the time
    zillow_key = ['X1-ZWz1fwjz1gd6a3_8w9nb',
                  'X1-ZWz1fxenyrfwgb_aroxz',
                  'X1-ZWz1933l3jqt57_aypsc']

    # we should have a list of states, for now, we'll work with new york state
    state = 'ny' 
    
    #step 1: get all the zip codes in the state. if we have a list of states, we need to loop through the states
    api_approval = None
    key_index = 0
    while api_approval != '0':
        if api_approval == None or key_index == 2:
            key_index = 0
        else:
            key_index += 1
        state_zip_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[key_index]+'&state='+state+'&childtype=zipcode'
        print('key_index:', key_index)
        state_zip_content = requests.get(state_zip_link).content
        state_zip_soup = BeautifulSoup(state_zip_content, 'lxml')

        api_approval = state_zip_soup.find_all('code')[0].get_text()
        print('getting approval code: ', api_approval)

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
    
    #Step 3: find all cities in the state
    api_approval = None
    while api_approval != '0':
        if api_approval == None or key_index == 2:
            key_index = 0
        else:
            key_index += 1
        state_city_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[key_index]+'&state='+state+'&childtype=city'
        print('key_index:', key_index)
        state_city_content = requests.get(state_city_link).content
        state_city_soup = BeautifulSoup(state_city_content, 'lxml')

        api_approval = state_city_soup.find_all('code')[0].get_text()
        print('getting approval code: ', api_approval)

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
            print('working on ', city)
            api_approval = None
            while api_approval != '0' and api_approval != '502':
                if api_approval == None or key_index == 2:
                    key_index = 0
                else:
                    key_index += 1
                state_city_zip_link = 'http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id='+zillow_key[key_index]+'&state='+state+'&city='+city+'&childtype=zipcode'
                
                state_city_zip_content = requests.get(state_city_zip_link).content
                state_city_zip_soup = BeautifulSoup(state_city_zip_content, 'lxml')

                api_approval = state_city_zip_soup.find_all('code')[0].get_text()
                print('getting approval code: ', api_approval)
                                
            if api_approval == '0':            
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
            
    print(df_state_city_zip_combined.head())
    print(df_state_city_zip_combined.shape)
    bookmark = input('bookmark') 
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    df_state_city_zip_combined_from_server = pd.read_sql('select * from state_city_zip;', engine)
    print(df_state_city_zip_combined_from_server.head())
    print(df_state_city_zip_combined_from_server.shape)
    
    bookmark = input('bookmark')    
    
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

if __name__ == '__main__':
    #some_df = zillow_init()
    zillow_zipcode_pull()
