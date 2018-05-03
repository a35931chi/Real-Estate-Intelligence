from fetch_other_data import get_fed_fund, get_mtg_rate, get_market, get_census, get_irs
from fetch_zillow_data import get_zri, get_zhvi

import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

#don't update SQL tables
#don't need to illustrate other data

#home prices portion
zhvi = get_zhvi(update = False)

#what variables are correlated with home value?
#select those only with 'Home Type' == 'All Homes'

mortgage_rate, mortgage_rate_annual = get_mtg_rate(False)

#find a zip code with the most data
#not sure what the offset in date should be
example_zips = list(zhvi[zhvi['Home Type'] == 'All Homes'].groupby('zip')['zhvi'].count().sort_values(ascending = False).index)[:5]


temp1_df = zhvi[(zhvi['zip'].isin(example_zips)) & (zhvi['Home Type'] == 'All Homes')]

temp2_df = pd.merge(temp1_df, mortgage_rate[['30 FRM Rate']], how = 'left',
                    left_index = True, right_index = True)


#is there a pos trend bias, as the rates keep dropping and home prices keeps increasing?
sns.lmplot(data = temp2_df, x = '30 FRM Rate', y = 'zhvi', hue = 'zip')
plt.tight_layout()
plt.show()

for zipcode in example_zips:
    print(zipcode)
    print(temp2_df[temp2_df.zip == zipcode][['30 FRM Rate', 'zhvi']].corr())
