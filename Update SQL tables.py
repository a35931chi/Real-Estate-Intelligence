import pandas as pd
import numpy as np
import quandl
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine, MetaData, String, Integer, Table, Column, ForeignKey

#assign all filenames
#new york state data
NY_zipname = r'New_York_State_ZIP_Codes-County_FIPS_Cross-Reference.csv'
US2010_zipname = r'E:\WinUser\Documents\Python Code\Zillow Fun\DEC_10_SF1_G001\DEC_10_SF1_G001_with_ann_small.csv'
#census data
USpop2010name = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data - 2010\DEC_10_SF1_H10_with_ann.csv'
USpop2000name = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data - 2000\DEC_00_SF1_H010_with_ann.csv'
#estimate data
USpop2011Ename = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data (Estimate) - 2011\ACS_11_5YR_B01003_with_ann.csv'
USpop2012Ename = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data (Estimate) - 2012\ACS_12_5YR_B01003_with_ann.csv'
USpop2013Ename = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data (Estimate) - 2013\ACS_13_5YR_B01003_with_ann.csv'
USpop2014Ename = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data (Estimate) - 2014\ACS_14_5YR_B01003_with_ann.csv'
USpop2015Ename = r'E:\WinUser\Documents\Python Code\Zillow Fun\US Census Population Data (Estimate) - 2015\ACS_15_5YR_B01003_with_ann.csv'

#load all data using pandas read_csv
NY_zip = pd.read_csv(NY_zipname, usecols = ['County Name','ZIP Code'], dtype = {'ZIP Code':str})
US2010_zip = pd.read_csv(US2010_zipname, names = ['zip', 'land', 'water', 'pop', 'housing', 'lat', 'long'],
                         skiprows = 1, dtype = {'zip': str})
USpop2000 = pd.read_csv(USpop2000name, names = ['tag1', 'zip', 'tag3', '2000'], skiprows = 2,
                        usecols = ['zip','2000'], dtype = {'zip': str})
USpop2010 = pd.read_csv(USpop2010name, names = ['tag1', 'zip', 'tag3', '2010'], skiprows = 2,
                        usecols = ['zip','2010'], dtype = {'zip': str})
USpop2011E = pd.read_csv(USpop2011Ename, names = ['tag1', 'zip', 'tag3', '2011', 'error'],
                         usecols = ['zip','2011','error'], skiprows = 2, na_values = '*****',
                         dtype = {'zip': str})
USpop2012E = pd.read_csv(USpop2012Ename, names = ['tag1', 'zip', 'tag3', '2012', 'error'],
                         usecols = ['zip','2012','error'], skiprows = 2, na_values = '*****',
                         dtype = {'zip': str})
USpop2013E = pd.read_csv(USpop2013Ename, names = ['tag1', 'zip', 'tag3', '2013', 'error'],
                         usecols = ['zip','2013','error'], skiprows = 2, na_values = '*****',
                         dtype = {'zip': str})
USpop2014E = pd.read_csv(USpop2014Ename, names = ['tag1', 'zip', 'tag3', '2014', 'error'],
                         usecols = ['zip','2014','error'], skiprows = 2, na_values = '*****',
                         dtype = {'zip': str})
USpop2015E = pd.read_csv(USpop2015Ename, names = ['tag1', 'zip', 'tag3', '2015', 'error'],
                         usecols = ['zip','2015','error'], skiprows = 2, na_values = '*****',
                         dtype = {'zip': str})


#merge data
combined = NY_zip.merge(US2010_zip, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop('zip', 1)

combined = combined.merge(USpop2000, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop('zip', 1)
combined = combined.merge(USpop2010, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop('zip', 1)
combined = combined.merge(USpop2011E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)
combined = combined.merge(USpop2012E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)
combined = combined.merge(USpop2013E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)
combined = combined.merge(USpop2014E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)
combined = combined.merge(USpop2015E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)
#combined['p_chg'] = combine['2010'].astype(np.float32)/combine['2000'].astype(np.float32) - 1
# density (people per km**2)
combined['2000_pop_density'] = combined['2000'] / combined['land'] * 1000000
combined['2010_pop_density'] = combined['2010'] / combined['land'] * 1000000
combined['2011_pop_density'] = combined['2011'] / combined['land'] * 1000000
combined['2012_pop_density'] = combined['2012'] / combined['land'] * 1000000
combined['2013_pop_density'] = combined['2013'] / combined['land'] * 1000000
combined['2014_pop_density'] = combined['2014'] / combined['land'] * 1000000
combined['2015_pop_density'] = combined['2015'] / combined['land'] * 1000000

print(combined.head())

combined.to_csv('out.csv', sep=',')





#write to csv and plot it on tableau
Quandlkey = '7ieY8tq_kjzWx2-DiyGx'
quandl.ApiConfig.api_key = Quandlkey

# this is one way of doing it. will make a very fat table
'''
home_value_data = pd.DataFrame(index = pd.date_range(start = '1995-01-01',
                                                     end = '2018-01-01',
                                                     freq = 'M'))

rental_value_data = pd.DataFrame(index = pd.date_range(start = '1995-01-01',
                                                       end = '2018-01-01',
                                                       freq = 'M'))  

for zip in NY_zip['ZIP Code']:
    hv_fn = 'ZILLOW/Z' + zip + '_ZHVIAH'
    rv_fn = 'ZILLOW/Z' + zip + '_ZRIAH'
    try:
        temp = quandl.get(hv_fn)
        home_value_data = home_value_data.join(temp, how = 'left')
        home_value_data.rename(columns = {'Value':zip}, inplace = True)
    except Exception as e:
        print(hv_fn + str(e))
    try:
        temp = quandl.get(rv_fn)
        rental_value_data = rental_value_data.join(temp, how = 'left')
        rental_value_data.rename(columns = {'Value':zip}, inplace = True)
    except Exception as e:
        print(rv_fn + str(e))

home_value_data.dropna(axis=0, how='all', inplace = True)
rental_value_data.dropna(axis=0, how='all', inplace = True)
'''
# this is another way of doing it. will make a very skinny table.
# this may be a more suitable solution for sql database
home_value_data = pd.DataFrame() 
rental_value_data = pd.DataFrame() 

error_count = 0
rerror_list = [] #1799 on 2017-07-27
herror_list = [] #1804 on 2017-07-27
for zip in NY_zip['ZIP Code']:
    hv_fn = 'ZILL/Z' + zip + '_A' #is it ZILL or ZILLOW?
    rv_fn = 'ZILL/Z' + zip + '_ZRIAH' #is it _A or _ZHVIAH, _RAH or _ZRIAH
    try:
        temp = quandl.get(hv_fn)
        temp['zip'] = zip
        home_value_data = pd.concat([home_value_data, temp], axis = 0)
    except Exception as e:
        print(hv_fn + str(e))
        error_count += 1
        herror_list.append(zip)
    try:
        temp = quandl.get(rv_fn)
        temp['zip'] = zip
        rental_value_data = pd.concat([rental_value_data, temp], axis = 0)
    except Exception as e:
        print(rv_fn + str(e))
        error_count += 1
        rerror_list.append(zip)
'''
engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
hsql_query = 'select * from home_value;'
htemp = pd.read_sql(hsql_query, engine, index_col = 'Date')

#home_value_data = pd.concat([home_value_data, htemp], axis = 0)
#home_value_data.drop_duplicates(inplace = True)

rsql_query = 'select * from rental_value;'
rtemp = pd.read_sql(rsql_query, engine, index_col = 'Date')

#rental_value_data = pd.concat([rental_value_data, rtemp], axis = 0)
#rental_value_data.drop_duplicates(inplace = True)

print(home_value_data.shape) #(131079, 2) on 2017-07-26
print(rental_value_data.shape) #(48533, 2) on 2017-07-26
print('{} errors out of {}'.format(error_count, 2*len(NY_zip)))
'''
'''
home_value_data.to_sql('home_value', engine, if_exists = 'replace', index = True)
rental_value_data.to_sql('rental_value', engine, if_exists = 'replace', index = True)
'''

#sql part
#input into sql table
'''
engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
conn = engine.raw_connection()
meta = MetaData(bind=engine)

#do i need to create the following tables or no? what the heck
table1 = Table('home_value', meta,
    Column('Date', String(10), primary_key=True, autoincrement=False),
    Column('Value', Integer, nullable=True),
    Column('zip', String(5), nullable=True))
table2 = Table('rental_value', meta,
    Column('Date', String(10), primary_key=True, autoincrement=False),
    Column('Value', Integer, nullable=True),
    Column('zip', String(5), nullable=True))    
meta.create_all(engine)


conn.close()

#read from sql table

engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
conn = engine.raw_connection()

sql_query = 'select * from home_value;'
df = pd.read_sql(sql_query, engine)
conn.close()
'''
