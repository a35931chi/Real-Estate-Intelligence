from fetch_other_data import get_fed_fund, get_mtg_rate, get_market, get_census, get_irs
from fetch_zillow_data import get_zri, get_zhvi

import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

from scipy import stats
from scipy.special import boxcox1p
from scipy.stats import norm, skew

import numpy as np

#don't update SQL tables
#don't need to illustrate other data

#home prices portion
zhvi = get_zhvi(update = False)

mortgage_rate, mortgage_rate_annual = get_mtg_rate(False)
mortgage_rate['30 FRM Rate pct_change'] = mortgage_rate['30 FRM Rate'].pct_change()
mortgage_rate['30 FRM Rate 1'] = mortgage_rate['30 FRM Rate'].shift(1)
mortgage_rate['30 FRM Rate 6'] = mortgage_rate['30 FRM Rate'].shift(6)
mortgage_rate['30 FRM Rate 12'] = mortgage_rate['30 FRM Rate'].shift(12)
mortgage_rate['30 FRM Rate 24'] = mortgage_rate['30 FRM Rate'].shift(24)

mortgage_rate['30 FRM Rate pct_change 1'] = mortgage_rate['30 FRM Rate pct_change'].shift(1)
mortgage_rate['30 FRM Rate pct_change 6'] = mortgage_rate['30 FRM Rate pct_change'].shift(6)
mortgage_rate['30 FRM Rate pct_change 12'] = mortgage_rate['30 FRM Rate pct_change'].shift(12)
mortgage_rate['30 FRM Rate pct_change 24'] = mortgage_rate['30 FRM Rate pct_change'].shift(24)

#find a zip code with the most data
#not sure what the offset in date should be

#what variables are correlated with home value?
#select those only with 'Home Type' == 'All Homes'

example_zips = list(zhvi[zhvi['Home Type'] == 'All Homes'].groupby('zip')['zhvi'].count().sort_values(ascending = False).index)[:5]


temp1_df = zhvi[(zhvi['zip'].isin(example_zips)) & (zhvi['Home Type'] == 'All Homes')]
temp1_df.sort_index().groupby(['Home Type', 'zip']).zhvi.pct_change().head(20)

temp1_df['zhvi pct_change'] = temp1_df.groupby(['Home Type', 'zip']).zhvi.pct_change()

temp2_df = pd.merge(temp1_df, mortgage_rate[['30 FRM Rate', '30 FRM Rate 1',
                                             '30 FRM Rate 6', '30 FRM Rate 12',
                                             '30 FRM Rate 24', '30 FRM Rate pct_change',
                                             '30 FRM Rate pct_change 1',
                                             '30 FRM Rate pct_change 6',
                                             '30 FRM Rate pct_change 12',
                                             '30 FRM Rate pct_change 24']],
                    how = 'left', left_index = True, right_index = True)

print(temp2_df[(temp2_df['zip'] == '12077') & (temp2_df['Home Type'] == 'All Homes')].head())


# looking at real values
#is there a pos trend bias, as the rates keep dropping and home prices keeps increasing?
sns.lmplot(data = temp2_df, x = '30 FRM Rate', y = 'zhvi', hue = 'zip')
plt.tight_layout()
plt.show()

for zipcode in example_zips:
    print(zipcode)
    heatmap1 = temp2_df[temp2_df.zip == zipcode][['zhvi', '30 FRM Rate', '30 FRM Rate 1',
                                                  '30 FRM Rate 6', '30 FRM Rate 12',
                                                  '30 FRM Rate 24']].corr()
    sns.heatmap(heatmap1, annot = True, cmap = "YlGnBu")
    plt.yticks(rotation = 0) 
    plt.title('zhvi vs. mortgage rate' + zipcode)
    plt.tight_layout()
    plt.show()

# looking at changes
sns.lmplot(data = temp2_df, x = '30 FRM Rate pct_change', y = 'zhvi pct_change', hue = 'zip')
plt.tight_layout()
plt.show()

for zipcode in example_zips:
    plt.scatter(x = temp2_df[temp2_df.zip == zipcode]['30 FRM Rate pct_change 24'],
                y = temp2_df[temp2_df.zip == zipcode]['zhvi pct_change'])
    plt.title(zipcode)
    plt.xlabel('30 FRM Rate pct_change')
    plt.ylabel('zhvi pct_change')
    plt.show()
                                                      

for zipcode in example_zips:
    print(zipcode)
    heatmap2 = temp2_df[temp2_df.zip == zipcode][['zhvi pct_change', '30 FRM Rate pct_change', '30 FRM Rate pct_change 1',
                                                  '30 FRM Rate pct_change 6', '30 FRM Rate pct_change 12',
                                                  '30 FRM Rate pct_change 24']].corr()
    sns.heatmap(heatmap2, annot = True, cmap = "YlGnBu")
    plt.yticks(rotation = 0) 
    plt.title('zhvi changes vs. mortgage rate changes' + zipcode)
    plt.tight_layout()
    plt.show()

temp3_df = temp2_df.groupby(['Home Type', 'zip'])['zhvi'].pct_change()

if True: #Target 'zhvi pct_change', trying to correct for skewness
    temp = temp2_df['zhvi pct_change'].dropna()
    lam = 0.0001
    fig, (ax1, ax2, ax3) = plt.subplots(ncols = 3, figsize = (15,6))
    sns.distplot(temp, fit = norm, ax = ax1)
    sns.distplot(boxcox1p(temp, lam), fit = norm, ax = ax2)
    sns.distplot(np.log1p(temp), fit = norm, ax = ax3)
    # Get the fitted parameters used by the function
    (mu1, sigma1) = norm.fit(temp)
    (mu2, sigma2) = norm.fit(temp, lam)
    (mu3, sigma3) = norm.fit(np.log1p(temp))
    ax1.legend(['Normal dist. ($\mu=$ {:.2f} and $\sigma=$ {:.2f} )'.format(mu1, sigma1),
                'Skewness: {:.2f}'.format(skew(temp))],
                loc = 'best')
    ax2.legend(['Normal dist. ($\mu=$ {:.2f} and $\sigma=$ {:.2f} )'.format(mu2, sigma2),
                'Skewness: {:.2f}'.format(skew(boxcox1p(temp, lam)))],
                loc = 'best')
    ax3.legend(['Normal dist. ($\mu=$ {:.2f} and $\sigma=$ {:.2f} )'.format(mu2, sigma3),
                'Skewness: {:.2f}'.format(skew(np.log1p(temp)))],
                loc = 'best')
    ax1.set_ylabel('Frequency')
    ax1.set_title('zhvi (% chg) Distribution')
    ax2.set_title('zhvi (% chg) Box-Cox Transformed')
    ax3.set_title('zhvi (% chg) Log Transformed')
    plt.tight_layout()
    #plt.savefig('zhvi (% chg) Distribution.png')
    plt.show()
