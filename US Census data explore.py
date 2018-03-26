import pandas as pd
import numpy as np

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
