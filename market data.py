import quandl
import numpy as np
import pandas as pd
import datetime
import matplotlib.pyplot as plt

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


#federal funds rate
def get_fed_fund(draw = False):
    current_key = quandl_init(None)
    temp = quandl.get('FED/RIFSPFF_N_M')
    temp.index = [datetime.datetime(i.year, i.month, 1) for i in temp.index]
    temp['year'] = temp.index.year
    temp.rename(columns = {'Value': 'Fed Rate'}, inplace = True)
    temp_annual = temp[['year', 'Fed Rate']].groupby(['year'])[['Fed Rate']].mean()
    temp_annual['Fed Rate pchg'] = temp_annual['Fed Rate'].pct_change()
    temp_annual['year_p1'] = temp_annual.index + 1
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp['Fed Rate'])
        axarr[0].set_title('monthly')
        axarr[1].plot(temp_annual['Fed Rate'])
        axarr[1].set_title('annual mean')
        plt.tight_layout()
        plt.show()
    return temp, temp_annual


#mortgage rate
def get_mtg_rate(draw = False):

    #'http://www.freddiemac.com/pmms/docs/30yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/15yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/5yr_pmmsmnth.xls'
    
    link = r'mortgage rates.xlsx'
    xls = pd.ExcelFile(link)
    temp = xls.parse(0, header = 0)
    temp['year'].fillna(method = 'ffill', inplace = True)
    temp['day'] = 1
    temp['Date'] = pd.to_datetime(temp[['year','month','day']])
    temp.drop(['month','day'], axis = 1, inplace = True)
    temp.set_index('Date', inplace = True)
    temp_annual = temp.groupby('year')[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']].mean()
    temp_annual['30 FRM Rate chg'] = temp_annual['30 FRM Rate'].pct_change()
    temp_annual['year_p1'] = temp_annual.index.astype(np.int32) + 1
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[0].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[0].set_title('monthly')
        axarr[1].plot(temp_annual[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[1].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[1].set_title('annual')
        plt.tight_layout()
        plt.show()
    return temp, temp_annual
    
#market movements
def get_market(draw = False):
    dow_link = '^DJI.csv'
    nasdaq_link = '^IXIC.csv'
    sp500_link = '^GSPC.csv'

    dow = pd.read_csv(dow_link).set_index('Date').add_suffix('_d')
    dow.index = pd.to_datetime(dow.index)
    nasdaq = pd.read_csv(nasdaq_link).set_index('Date').add_suffix('_n')
    nasdaq.index = pd.to_datetime(nasdaq.index)
    sp500 = pd.read_csv(sp500_link).set_index('Date').add_suffix('_s')
    sp500.index = pd.to_datetime(sp500.index)

    temp = dow.merge(nasdaq, how = 'left', left_index = True, right_index = True).merge(sp500, how = 'left', left_index = True, right_index = True)
    temp['year'] = temp.index.year
    
    temp_annual = (temp[['year', 'Adj Close_d', 'Adj Close_n', 'Adj Close_s']].groupby(['year'])[['Adj Close_d', 'Adj Close_n', 'Adj Close_s']].mean()).merge((temp[['year', 'Adj Close_d', 'Adj Close_n', 'Adj Close_s']].groupby(['year'])[['Adj Close_d', 'Adj Close_n', 'Adj Close_s']].last() / temp[['year', 'Adj Close_d', 'Adj Close_n', 'Adj Close_s']].groupby(['year'])[['Adj Close_d', 'Adj Close_n', 'Adj Close_s']].first() - 1), how = 'left', left_index = True, right_index = True, suffixes = ('_avg', '_pct_chg'))    
    temp_annual.rename(columns = {'Adj Close_d_avg':'DOW avg',
                                  'Adj Close_n_avg':'NASDAQ avg',
                                  'Adj Close_s_avg':'SP500 avg',
                                  'Adj Close_d_pct_chg':'DOW perf',
                                  'Adj Close_n_pct_chg':'NASDAQ perf',
                                  'Adj Close_s_pct_chg':'SP500 perf'},
                       inplace = True)
    temp_annual[['DOW pchg', 'NASDAQ pchg', 'SP500 pchg']] = temp_annual[['DOW avg', 'NASDAQ avg', 'SP500 avg']].pct_change()
    temp_annual['year_p1'] = temp_annual.index + 1
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp[['Adj Close_d', 'Adj Close_s', 'Adj Close_n']])
        axarr[0].legend(['DOW', 'SP500', 'NASDAQ'])
        axarr[0].set_title('monthly')
        axarr[1].plot(temp_annual[['DOW avg', 'SP500 avg', 'NASDAQ avg']])
        axarr[1].legend(['DOW', 'SP500', 'NASDAQ'])
        axarr[1].set_title('annual')
        plt.tight_layout()
        plt.show()

    return temp, temp_annual

    
if __name__ == '__main__':
    fed_rate, fed_rate_annual = get_fed_fund(True)
    mortgage_rate, mortgage_rate_annual = get_mtg_rate(True)
    market, market_annual = get_market(True)
