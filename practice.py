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
from string import punctuation
import re

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
        state_zip_content = requests.get(state_zip_link).content
        state_zip_soup = BeautifulSoup(state_zip_content, 'lxml')

        api_approval = state_zip_soup.find_all('code')[0].get_text()
        #print('getting approval code: ', api_approval)

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
        state_city_content = requests.get(state_city_link).content
        state_city_soup = BeautifulSoup(state_city_content, 'lxml')

        api_approval = state_city_soup.find_all('code')[0].get_text()
        #print('getting approval code: ', api_approval)

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
                #print('getting approval code: ', api_approval)
                                
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

    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
        
    old = pd.read_sql('select * from state_city_zip;', engine, index_col = 'index')
    old.drop_duplicates(subset = ['state_city_zip_Name','state_city_zip_ID'], inplace = True)
    print('This is our old database shape: '.format(old.shape)) #(1690, 6) as of 2018-04-24
        
    new = df_state_city_zip_combined.copy()
    new.drop_duplicates(subset = ['state_city_zip_Name','state_city_zip_ID'], inplace = True)
    print('This is our new database shape: '.format(new.shape)) #(1702, 6) as of 2018-04-24
    
    return old, new


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

    print('Current zip file shape: {}'.format(df_state_city_zip_combined.shape))
    return df_state_city_zip_combined

def zhvi(df_state_city_zip_combined):
    #zillow home price index portion
    zhvi = pd.DataFrame()
    print('Getting data from Zillow API...')
    for link in df_state_city_zip_combined['zhvi_url']:
        
        try:
            xls = pd.ExcelFile(link)
            #print(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                zhvi = pd.concat([zhvi, temp], axis = 0)
        except Exception as e:
            print(link)
            print(e)

    # quandl zhvi
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
        
    df_quandl_zhvi = pd.DataFrame()

    current_key = quandl_init(None)
    print('Getting data from Quandl API...')

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

    print('from Zillow API:')
    print('original pre-tranformed shape: {}'.format(zhvi.shape)) #from link (18752, 123) as of 2018-04-25
    #print(zhvi.duplicated(keep = 'first').sum()) #13467
    #print(zhvi.duplicated(keep = 'first').sum())

    #transpose this dataframe
    zhvi_T = zhvi.set_index(['Data Type', 'Region Name', 'Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'zhvi'})
    zhvi_T['Region Name'] = zhvi_T['Region Name'].apply(lambda x: str(x).zfill(5))
    #we need to drop some columns and rename some columns
    zhvi_T.rename(columns = {'Data Type': 'Home Type', 'Region Name': 'zip'}, inplace = True)
    zhvi_T.drop(['Region Type'], inplace = True, axis = 1)
    print('original tranformed shape: {}'.format(zhvi_T.shape)) #(632589, 4)
    zhvi_T.drop_duplicates(keep = 'first', inplace = True) #
    print('deduped shape: {}'.format(zhvi_T.shape)) #(1103534, 3)

    print('from Quandl API:')
    print('original shape: {}'.format(df_quandl_zhvi.shape)) #(1417937, 3)
    #print(df_quandl_zhvi.duplicated(keep = 'first').sum()) #278921
    df_quandl_zhvi.drop_duplicates(keep = 'first', inplace = True)
    print('deduped shape: {}'.format(df_quandl_zhvi.shape)) #(1139016, 3)
    df_quandl_zhvi.index.name = 'Date'
    df_quandl_zhvi.reset_index(inplace = True)

    return zhvi_T, df_quandl_zhvi


def zri(df_state_city_zip_combined):
    #zillow rental index portion
    zri = pd.DataFrame()
    print('Getting data from Zillow API...')

    for link in df_state_city_zip_combined['zri_url']:
        #print(link)
        try:
            xls = pd.ExcelFile(link)
            for i in range(len(xls.sheet_names)):
                temp = xls.parse(i, header=1)
                temp.columns.values[3:] = [datetime.datetime(i.year, i.month, 1) for i in temp.columns.values[3:]]
                zri = pd.concat([zri, temp], axis = 0) 
        except Exception as e:
            print(link)
            print(e)

    #Zillow Rental Index -
    quandl_zri = {'ZRIAHMF': 'All Homes Multi Fam',
                  'ZRIAH': 'All Homes',
                  'ZRIMFRR': 'Multi Fam',
                  'ZRISFRR': 'Single Fam'}
        
    df_quandl_zri = pd.DataFrame()

    current_key = quandl_init(None)
    print('Getting data from Quandl API...')

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


    print('from Zillow API:')
    print('original pre-transformed shape: {}'.format(zri.shape)) #shape as of 2018-04-25 original shape: (11137, 92)
    #print(zhvi.duplicated(keep = 'first').sum()) #
    #print(zhvi.duplicated(keep = 'first').sum())

    #transpose this dataframe
    zri_T = zri.set_index(['Data Type', 'Region Name', 'Region Type']).stack().reset_index().rename(columns = {'level_3': 'Date', 0: 'zri'})
    zri_T['Region Name'] = zri_T['Region Name'].apply(lambda x: str(x).zfill(5))
    #we need to drop some columns and rename some columns
    zri_T.rename(columns = {'Data Type': 'Home Type', 'Region Name': 'zip'}, inplace = True)
    zri_T.drop(['Region Type'], inplace = True, axis = 1)
    print('original transformed shape: {}'.format(zri_T.shape)) #(954433, 4)
    zri_T.drop_duplicates(keep = 'first', inplace = True) #
    print('deduped shape: {}'.format(zri_T.shape)) #deduped shape: (224976, 4)
    

    print('from Quandl API:')
    print('original shape: {}'.format(df_quandl_zri.shape)) #original shape: (179477, 3)
    #print(df_quandl_zri.duplicated(keep = 'first').sum()) #
    df_quandl_zri.drop_duplicates(keep = 'first', inplace = True)
    #print(df_quandl_zri.shape) #(1139016, 3)
    df_quandl_zri.index.name = 'Date'
    df_quandl_zri.reset_index(inplace = True)
    print('deduped shape: {}'.format(df_quandl_zri.shape)) #deduped shape: (155080, 4)

    return zri_T, df_quandl_zri

def zipcode_update():
    old_zips, new_zips = zillow_zipcode_pull()

    merged = old_zips.merge(new_zips, indicator = True, how = 'outer', on = 'state_city_zip_Name')

    print("When comparing old vs. new, here's the overlaping situation: {}".format(set(merged['_merge']))) #{'both', 'right_only'} as of 2018-04-24
    
    print('{} data that exists in both old and new tables.'.format(merged[merged._merge == 'both'].shape)) # (1694, 12) as of 2018-04-24
    print('{} locality ID that differs'.format(merged[(merged._merge == 'both') & (merged.state_city_zip_ID_x != merged.state_city_zip_ID_y)].shape)) #(0, 12) as of 2018-04-24
    print('{} state that differs'.format(merged[(merged._merge == 'both') & (merged.state_x != merged.state_y)].shape)) #(0, 12)
    print('{} city that differs'.format(merged[(merged._merge == 'both') & (merged.city_x != merged.city_y)].shape)) #(0, 12)
    print('{} latitude that differs'.format(merged[(merged._merge == 'both') & (merged.state_city_zip_Lat_x != merged.state_city_zip_Lat_y)].shape)) #(467, 12)
    print('{} longitude that differs'.format(merged[(merged._merge == 'both') & (merged.state_city_zip_Long_x != merged.state_city_zip_Long_y)].shape)) #(1067, 12)

    print('{} data that exists in only new tables.'.format(merged[merged._merge == 'right_only'].shape)) # (14, 12) as of 2018-04-24
    print(merged[merged._merge == 'right_only'].head())

    bookmark = input('Are you sure you want to update SQL table? (Y/N)')
    if bookmark == 'Y':
        engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
        new_zips.to_sql('state_city_zip', engine, if_exists = 'replace', index = False)
        print('SQL table Update Completed')
    else:
        print('SQL table was not updated')
    pass

def get_zhvi(update = False):
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    oldz = pd.read_sql('select * from zillow_zhvi;', engine)
    #oldz.columns = [re.search(r'\(?([0-9A-Za-z]+)\)?', col).group(1) for col in oldz.columns]
    oldz.columns = ['Date', 'Home Type', 'zip', 'zhvi']
    
    if update:
        df_state_city_zip_links = zillow_init()

        zhvi_zillow, quandl_zhvi = zhvi(df_state_city_zip_links)
        #zhvi_zillow as of 2018-04-30 (632589, 4)
        #quandl_zhvi as of 2018-04-30 
        #oldz as of 2018-04-30 (928125, 4)

        zhvi_zillow['source'] = 'Z'
        quandl_zhvi['source'] = 'Q'
        oldz['source'] = 'S'

        temp = pd.concat([oldz, zhvi_zillow, quandl_zhvi], axis = 0) #(2699730, 5)
        temp = temp.set_index(['Date', 'Home Type', 'zip', 'source']).unstack(level = -1).reset_index()

        temp['p_diff'] = (temp.xs('zhvi', level = 0, axis = 1).max(axis = 1) - temp.xs('zhvi', level = 0, axis = 1).min(axis = 1)) / temp.xs('zhvi', level = 0, axis = 1).mean(axis = 1)
        
        #my rule:
        #1. take Zillow's answer, if Zillow not missing
        #2. take SQL's answer, if Zillow is missing and SQL is not missing
        #3. take Quandl's answer, if both Zillow and SQL is missing
        
        def condense_zhvi(df):
            if ~pd.isnull(df['Z']):
                return df['Z']
            elif ~pd.isnull(df['S']):
                return df['S']
            else:
                return df['Q']

        temp['zhvi_final'] = temp['zhvi'].apply(condense_zhvi, axis = 1)

        #just checking
        print(temp[temp.xs('Z', level= 1, axis=1).isnull()['zhvi']].head())
        print(temp[(temp.xs('Z', level= 1, axis=1).isnull()['zhvi']) & temp.xs('S', level= 1, axis=1).isnull()['zhvi']].head())
        print(temp[temp.xs('Q', level= 1, axis=1).isnull()['zhvi']].head())
        
        tempa = temp[['Date', 'Home Type', 'zip', 'zhvi_final']].rename(columns = {'zhvi_final': 'zhvi'})
        bookmark = input('Are you sure you want to update SQL table? (Y/N)')
        if bookmark == 'Y':
            engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
            tempa.to_sql('zillow_zhvi', engine, if_exists = 'replace', index = False)
            print('SQL table Update Completed')
        else:
            print('SQL table was not updated')
    else:
        tempa = oldz
        
    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            #print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    return temp1

def get_zri(update = False):
    engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
    oldz = pd.read_sql('select * from zillow_zri;', engine)
    #oldz.columns = [re.search(r'\(?([0-9A-Za-z]+)\)?', col).group(1) for col in oldz.columns]
    
    if update:
        df_state_city_zip_links = zillow_init()

        zillow_zri, quandl_zri = zri(df_state_city_zip_links)
        #zillow_zri as of 2018-04-30 (224976, 4)
        #quandl_zri as of 2018-04-30(155080, 4)
        #oldz as of 2018-04-30 (221395, 4)

        zillow_zri['source'] = 'Z'
        quandl_zri['source'] = 'Q'
        oldz['source'] = 'S'

        temp = pd.concat([oldz, zillow_zri, quandl_zri], axis = 0) #(601451, 5)
        temp = temp.set_index(['Date', 'Home Type', 'zip', 'source']).unstack(level = -1).reset_index()

        temp['p_diff'] = (temp.xs('zri', level = 0, axis = 1).max(axis = 1) - temp.xs('zri', level = 0, axis = 1).min(axis = 1)) / temp.xs('zri', level = 0, axis = 1).mean(axis = 1)
        
        #my rule:
        #1. take Zillow's answer, if Zillow not missing
        #2. take SQL's answer, if Zillow is missing and SQL is not missing
        #3. take Quandl's answer, if both Zillow and SQL is missing
        
        def condense_zri(df):
            if ~pd.isnull(df['Z']):
                return df['Z']
            elif ~pd.isnull(df['S']):
                return df['S']
            else:
                return df['Q']

        temp['zri_final'] = temp['zri'].apply(condense_zri, axis = 1)

        #just checking
        print(temp[temp.xs('Z', level= 1, axis=1).isnull()['zri']].head())
        print(temp[(temp.xs('Z', level= 1, axis=1).isnull()['zri']) & temp.xs('S', level= 1, axis=1).isnull()['zri']].head())
        print(temp[temp.xs('Q', level= 1, axis=1).isnull()['zri']].head())
        
        tempa = temp[['Date', 'Home Type', 'zip', 'zri_final']].rename(columns = {'zri_final': 'zri'})
        bookmark = input('Are you sure you want to update SQL table? (Y/N)')
        if bookmark == 'Y':
            engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
            tempa.to_sql('zillow_zri', engine, if_exists = 'replace', index = False)
            print('SQL table Update Completed')
        else:
            print('SQL table was not updated')
    else:
        tempa = oldz
        
    #proceed to fill in the data with interpolated values
    temp1 = pd.DataFrame()

    for zipcode in tempa['zip'].unique():
        for home_type in tempa[tempa['zip'] == zipcode]['Home Type'].unique():
            #print(zipcode, home_type)
            df = tempa[(tempa['zip'] == zipcode) & (tempa['Home Type'] == home_type)]
            df.set_index('Date', inplace = True)
            df = df.reindex(pd.date_range(start = df.index.min(), end = df.index.max(), freq = 'MS'), fill_value = np.nan)
            mask = df['zip'].isnull()
            df = df.interpolate(method = 'linear', axis = 0).ffill()
            df['interp'] = mask
            temp1 = pd.concat([temp1, df])

    return temp1
    
            
if __name__ == '__main__':
    #the only three things you should call:
    #1. zipcode_update()
    #2. get_zhvi(if you want to pull new info, then True)
    #3. get_zri(if you want to pull new info, then True)

    #df = get_zhvi(False) pulls old zhvi data, as of 5/1/2018 (1619288, 4)
    #df = get_zhvi(True) pulls new zhvi data, tries to update SQL, as of 5/1/2018 (1730873, 4)

    #df = get_zri(False) pulls old zri data, as of 5/1/2018 (221395, 4)
    #df = get_zri(True) pulls new zri data, tries to update SQL, as of 5/1/2018 (1619288, 4)
    df = get_zri(True)

