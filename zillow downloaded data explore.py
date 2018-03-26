#let's first play with readily available data from zillow
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# first look at most current data
NY_zip_name = r'New_York_State_ZIP_Codes-County_FIPS_Cross-Reference.csv'
NY_zip = pd.read_csv(NY_zip_name, usecols = ['County Name','ZIP Code'], dtype = {'ZIP Code':str})


rental_price_psf_name = r'Zip_MedianRentalPricePerSqft_AllHomes.csv'
home_value_psf_name = r'Zip_MedianValuePerSqft_AllHomes.csv'

rental_price_psf = pd.read_csv(rental_price_psf_name)
home_value_psf = pd.read_csv(home_value_psf_name)

combined = home_value_psf.merge(rental_price_psf, how = 'outer',
                                on = 'RegionName', suffixes = ('_h','_r'))

'''
current_hvi = pd.read_csv('Zip_Zhvi_Summary_AllHomes.csv')
#export for tableau visualization

#time series home prices by zip codes
hvi = pd.read_csv('Zip_Zhvi_AllHomes.csv') 
hvi_historical = hvi.set_index('RegionName').T[[10065]][6:] 
hvi_historical.index = pd.to_datetime(hvi_historical.index, format = '%Y-%m')

#time series rental prices by zip codes
ri = pd.read_csv('Zip_Zri_AllHomesPlusMultifamily.csv')
ri_historical = ri.set_index('RegionName').T[10065][6:]
ri_historical.index = pd.to_datetime(ri_historical.index, format = '%Y-%m')
#let's look at the ratio
ratio = ri_historical / hvi_historical

f, axarr = plt.subplots(3, sharex=True, figsize = (13,6))
axarr[0].plot(hvi_historical)
axarr[0].set_title('Avg Home Prices v. Rental Prices v. Ratio')
axarr[1].plot(ri_historical)
axarr[2].plot(ratio)
plt.show()

#using seaborn, can we see a trend?
sns.lmplot(data = hvi_historical, y = 10065, x = hvi_historical.index)
plt.show()

#figure out all 5 borough's zip codes, and see if we can plot trends for all 5 borough


#look at the rate of growth in the past year, home value, rent, and index.
'''
