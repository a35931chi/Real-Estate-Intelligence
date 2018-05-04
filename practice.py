from fetch_other_data import get_fed_fund, get_mtg_rate, get_market, get_census, get_irs
from fetch_zillow_data import get_zri, get_zhvi

import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

#don't update SQL tables
#don't need to illustrate other data

#home prices portion
zhvi = get_zhvi(update = False)

mortgage_rate, mortgage_rate_annual = get_mtg_rate(False)
mortgage_rate['30 FRM Rate 1'] = mortgage_rate['30 FRM Rate'].shift(1)
mortgage_rate['30 FRM Rate 6'] = mortgage_rate['30 FRM Rate'].shift(6)
mortgage_rate['30 FRM Rate 12'] = mortgage_rate['30 FRM Rate'].shift(12)
mortgage_rate['30 FRM Rate 24'] = mortgage_rate['30 FRM Rate'].shift(24)

#find a zip code with the most data
#not sure what the offset in date should be

#what variables are correlated with home value?
#select those only with 'Home Type' == 'All Homes'

example_zips = list(zhvi[zhvi['Home Type'] == 'All Homes'].groupby('zip')['zhvi'].count().sort_values(ascending = False).index)[:5]


temp1_df = zhvi[(zhvi['zip'].isin(example_zips)) & (zhvi['Home Type'] == 'All Homes')]

temp2_df = pd.merge(temp1_df, mortgage_rate[['30 FRM Rate', '30 FRM Rate 1',
                                             '30 FRM Rate 6', '30 FRM Rate 12',
                                             '30 FRM Rate 24']],
                    how = 'left', left_index = True, right_index = True)

print(temp2_df[(temp2_df['zip'] == '12077') & (temp2_df['Home Type'] == 'All Homes')].head())


#is there a pos trend bias, as the rates keep dropping and home prices keeps increasing?
sns.lmplot(data = temp2_df, x = '30 FRM Rate', y = 'zhvi', hue = 'zip')
plt.tight_layout()
plt.show()

for zipcode in example_zips:
    print(zipcode)
    heatmap_df = temp2_df[temp2_df.zip == zipcode][['zhvi', '30 FRM Rate', '30 FRM Rate 1',
                                             '30 FRM Rate 6', '30 FRM Rate 12',
                                             '30 FRM Rate 24']].corr()
    sns.heatmap(heatmap_df, annot = True, cmap = "YlGnBu")
    plt.yticks(rotation = 0) 
    plt.title('zhvi vs. mortgage rate')
    plt.tight_layout()
    plt.show()
