from pyzillow.pyzillow import ZillowWrapper, GetDeepSearchResults, GetUpdatedPropertyDetails
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

api_key = 'X1-ZWz1fwjz1gd6a3_8w9nb'


#http://www.zillow.com/webservice/GetDeepSearchResults.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&address=350+E62nd+Street+Apt+4J&citystatezip=New+York%2C+NY
address1 = '350+E62nd+Street+Apt+4J, New+York, NY'
zip1 = '10065'
#result_deep.zillow_id '31537611'

address2 = '301+East+61st+Street+#2B, New+York, NY'
zip2 = '10065'
#http://www.zillow.com/webservice/GetDeepSearchResults.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&address=1982+N+Maud+Ave+Apt+i&citystatezip=Chicago%2C+IL
address3 = '1982+N+Maud+Ave+Apt+i, Chicago, IL'
#http://www.zillow.com/webservice/GetDeepSearchResults.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&address=110+River+Drive+Apt+608&citystatezip=Jersey+City%2C+NJ
address4 = '110+River+Drive+Apt+608, Jersey+City, NJ'
deep_search_response = ZillowWrapper(api_key).get_deep_search_results(address2, zip2)
result_deep = GetDeepSearchResults(deep_search_response)
#result_updated = GetUpdatedPropertyDetails(deep_search_response)

print(result_deep.home_detail_link)

'''
area_unit 'SqFt'
attribute_mapping
bathrooms
bedrooms
data
get_attr
graph_data_link
home_detail_link
home_size
home_type
last_sold_date
last_sold_price
last_sold_price_currency 'USD'
latitude
longitude
map_this_home_link
property_size
tax_value
tax_year
year_built
zestimate_amount
zestimate_last_updated
zestimate_percentile
zestimate_valuationRange_low
zestimate_valuation_range_high
zestimate_value_change
zillow_id


import requests
from bs4 import BeautifulSoup
url = result_deep.graph_data_link
r = requests.get(url)




link = 'https://www.zillow.com/ajax/homedetail/HomeValueChartData.htm?mt=1&zpid='+result_deep.zillow_id
raw_file = requests.get(link).content

df = []
for row in raw_file.split('\n'):
    df.append(row.split('\t'))
df = pd.DataFrame(df[1:], columns = df[0]).set_index('Date', drop=True).dropna()
df['Value'] = df['Value'].astype(np.int32)

for content in set(df['Label']):
    df[df['Label'] == content]['Value'].astype(np.int32).plot(label = content)
plt.legend()
plt.show()
''' 
