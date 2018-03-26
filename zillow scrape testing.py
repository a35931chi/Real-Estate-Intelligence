from pyzillow.pyzillow import ZillowWrapper, GetDeepSearchResults, GetUpdatedPropertyDetails
import requests
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

ZWSID = 'X1-ZWz1fwjz1gd6a3_8w9nb'

#lets try scraping this:
zipscrape = 'https://www.zillow.com/new-york-ny-11201/home-values/'

raw_file = requests.get(zipscrape).content

'''
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


#example: http://www.zillow.com/webservice/GetRegionChildren.htm?zws-id=X1-ZWz1fwjz1gd6a3_8w9nb&state=ny&city=new+york&childtype=neighborhood
