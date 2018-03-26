import pandas as pd
import numpy as np
import pymysql
import pymysql.cursors
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://a35931chi:Maggieyi66@localhost/realestate')
hsql_query = 'select * from home_value;'
htemp = pd.read_sql(hsql_query, engine, index_col = 'Date')
print('{} = {}'.format(htemp.shape, htemp.drop_duplicates().shape)) #??

#htemp = htemp.sortlevel(1, 0) #may be unnecessary

zips = htemp['zip'].unique()
hdf = pd.DataFrame()
for zip in zips: #hdf.shape = (163021, 1) on 2017-07-27
    df = pd.DataFrame(index = pd.date_range(start = min(htemp[htemp['zip'] == zip].index),
                                            end = max(htemp[htemp['zip'] == zip].index),
                                            freq = 'M'))
    df['zip'] = zip
    df.index.name = 'Date'
    df = pd.merge(df.reset_index(), htemp[htemp['zip'] == zip].reset_index(),
               how = 'left', left_on = ['Date', 'zip'],
               right_on = ['Date', 'zip'])
    df['Value_i'] = df['Value'].interpolate(method = 'linear', axis = 0)
    
    hdf = pd.concat([hdf, df], axis = 0)

hdf.set_index(['zip','Date'], inplace= True)
#want to rethink how trend is measured in real estate? is it about the price change in one year?
hdf['pchg'] = hdf.groupby(level = 0)['Value_i'].pct_change(periods = 12)

rsql_query = 'select * from rental_value;'
rtemp = pd.read_sql(rsql_query, engine, index_col = 'Date')
print('{} = {}'.format(rtemp.shape, rtemp.drop_duplicates().shape)) #??

zips = rtemp['zip'].unique()
rdf = pd.DataFrame()
for zip in zips: #hdf.shape = (55300, 4) on 2017-07-27
    df = pd.DataFrame(index = pd.date_range(start = min(rtemp[rtemp['zip'] == zip].index),
                                            end = max(rtemp[rtemp['zip'] == zip].index),
                                            freq = 'M'))
    df['zip'] = zip
    df.index.name = 'Date'
    df = pd.merge(df.reset_index(), rtemp[rtemp['zip'] == zip].reset_index(),
               how = 'left', left_on = ['Date', 'zip'],
               right_on = ['Date', 'zip'])
    df['Value_i'] = df['Value'].interpolate(method = 'linear', axis = 0)
    
    rdf = pd.concat([rdf, df], axis = 0)

rdf.set_index(['zip','Date'], inplace= True)
rdf['pchg'] = rdf.groupby(level = 0)['Value_i'].pct_change(periods = 12)


#bookmark


combined = pd.merge(hdf.reset_index(), rdf.reset_index(), how = 'left',
                    left_on = ['Date','zip'], right_on = ['Date', 'zip'],
                    suffixes = ('_h','_r'))

combined['rvh'] = combined['Value_i_r'] / combined['Value_i_h']*12
combined.set_index('Date', inplace = True)
print(combined.head())


combined.to_csv('output.csv')


htemp.xs('10001', level = 1).head()
htemp['pct_change_12m'] = htemp.groupby(level = 
htemp.xs('12204', level = 1)

#interpolate method:
hdf[hdf['zip'] == '12203'].interpolate(method = 'linear', axis = 0).head(20)

