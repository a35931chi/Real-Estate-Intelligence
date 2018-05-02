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
    print('Updated by: Quandl')
    current_key = quandl_init(None)
    temp = quandl.get('FED/RIFSPFF_N_M')
    temp.index = [datetime.datetime(i.year, i.month, 1) for i in temp.index]
    temp['year'] = temp.index.year
    temp.rename(columns = {'Value': 'Fed Rate'}, inplace = True)
    temp_annual = temp[['year', 'Fed Rate']].groupby(['year'])[['Fed Rate']].mean()
    temp_annual['Fed Rate pchg'] = temp_annual['Fed Rate'].pct_change()
    temp_annual['year_p1'] = temp_annual.index + 1
    print('date range: {} to {}'.format(temp.index[0], temp.index[-1]))
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp['Fed Rate'])
        axarr[0].set_title('monthly', fontsize = 8)
        axarr[1].plot(temp_annual['Fed Rate'])
        axarr[1].set_title('annual mean', fontsize = 8)
        figure_title = 'Fed Funds Rates'
        plt.text(0.5, 2.42, figure_title,
                 horizontalalignment = 'center',
                 fontsize = 14,
                 transform = axarr[1].transAxes)
        plt.tight_layout()
        plt.show()
    
    return temp, temp_annual


#mortgage rate
def get_mtg_rate(draw = False):

    #'http://www.freddiemac.com/pmms/docs/30yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/15yr_pmmsmnth.xls'
    #'http://www.freddiemac.com/pmms/docs/5yr_pmmsmnth.xls'
    print('Updated by: freddiemac')
    link = 'Data/mortgage_rates.xlsx'
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
    print('date range: {} to {}'.format(temp.index[0], temp.index[-1]))
    if draw:
        f, axarr = plt.subplots(2, sharex = False)
        axarr[0].plot(temp[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[0].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[0].set_title('monthly', fontsize = 8)
        axarr[1].plot(temp_annual[['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate']])
        axarr[1].legend(['30 FRM Rate', '15 FRM Rate', '5-1 ARM Rate'])
        axarr[1].set_title('annual', fontsize = 8)
        plt.suptitle('Mortgage Rates')
        plt.tight_layout()
        plt.show()
    return temp, temp_annual
    
#market movements
def get_market(draw = False):
    #https://query1.finance.yahoo.com/v7/finance/download/%5EDJI?period1=946702800&period2=1525233600&interval=1d&events=history&crumb=KvNPupdi4vA
    #https://query1.finance.yahoo.com/v7/finance/download/%5EIXIC?period1=946702800&period2=1525233600&interval=1d&events=history&crumb=KvNPupdi4vA
    #https://query1.finance.yahoo.com/v7/finance/download/%5EGSPC?period1=946702800&period2=1525233600&interval=1d&events=history&crumb=KvNPupdi4vA
    #to add a day, add 86400 to period2
    print('Updated by: Yahoo! Finance')
    dow_link = 'Data/^DJI.csv'
    nasdaq_link = 'Data/^IXIC.csv'
    sp500_link = 'Data/^GSPC.csv'

    dow = pd.read_csv(dow_link).set_index('Date').add_suffix('_d')
    dow.index = pd.to_datetime(dow.index)
    nasdaq = pd.read_csv(nasdaq_link).set_index('Date').add_suffix('_n')
    nasdaq.index = pd.to_datetime(nasdaq.index)
    sp500 = pd.read_csv(sp500_link).set_index('Date').add_suffix('_s')
    sp500.index = pd.to_datetime(sp500.index)

    temp = dow.merge(nasdaq, how = 'left', left_index = True, right_index = True).merge(sp500, how = 'left', left_index = True, right_index = True)
    temp['year'] = temp.index.year
    print('date range: {} to {}'.format(temp.index[0], temp.index[-1]))
    
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
        plt.suptitle('Market Movements')
        plt.show()

    return temp, temp_annual

#census data
def get_census():
    #assign all filenames
    #new york state data
    #need to reference these files to fetch locations
    print('Updated by: American Community Survey (US Census Bureau)')
    #https://data.ny.gov/Government-Finance/New-York-State-ZIP-Codes-County-FIPS-Cross-Referen/juva-r6g2
    NY_zipname = 'Data/New_York_State_ZIP_Codes-County_FIPS_Cross-Reference.csv' #November 4, 2015
    
    US2010_zipname = 'Data/DEC_10_SF1_G001_with_ann_small.csv'
    #census data
    USpop2000name = 'Data/Census_Pop_Data_2000/DEC_00_SF1_H010_with_ann.csv'
    USpop2010name = 'Data/Census_Pop_Data_2010/DEC_10_SF1_H10_with_ann.csv'

    #estimate data
    #go to https://factfinder.census.gov/, advanced search, show me all,
    #geographies = all 5-digit zip codes (860)
    #topics = people, basic count/estimate, population total
    #look for B01003 (Total population)
    USpop2011Ename = 'Data/Census_Pop_Data_2011E/ACS_11_5YR_B01003_with_ann.csv'
    USpop2012Ename = 'Data/Census_Pop_Data_2012E/ACS_12_5YR_B01003_with_ann.csv'
    USpop2013Ename = 'Data/Census_Pop_Data_2013E/ACS_13_5YR_B01003_with_ann.csv'
    USpop2014Ename = 'Data/Census_Pop_Data_2014E/ACS_14_5YR_B01003_with_ann.csv'
    USpop2015Ename = 'Data/Census_Pop_Data_2015E/ACS_15_5YR_B01003_with_ann.csv'
    USpop2016Ename = 'Data/Census_Pop_Data_2016E/ACS_16_5YR_B01003_with_ann.csv'

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
    USpop2016E = pd.read_csv(USpop2016Ename, names = ['tag1', 'zip', 'tag3', '2016', 'error'],
                             usecols = ['zip','2016','error'], skiprows = 2, na_values = '*****',
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
    combined = combined.merge(USpop2016E, how = 'left', left_on = 'ZIP Code', right_on = 'zip').drop(['zip','error'], 1)

    #combined['p_chg'] = combine['2010'].astype(np.float32)/combine['2000'].astype(np.float32) - 1
    # density (people per km**2)
    combined['2000_pop_density'] = combined['2000'] / combined['land'] * 1000000
    combined['2010_pop_density'] = combined['2010'] / combined['land'] * 1000000
    combined['2011_pop_density'] = combined['2011'] / combined['land'] * 1000000
    combined['2012_pop_density'] = combined['2012'] / combined['land'] * 1000000
    combined['2013_pop_density'] = combined['2013'] / combined['land'] * 1000000
    combined['2014_pop_density'] = combined['2014'] / combined['land'] * 1000000
    combined['2015_pop_density'] = combined['2015'] / combined['land'] * 1000000
    combined['2016_pop_density'] = combined['2016'] / combined['land'] * 1000000

    return combined

def get_irs(draw = False):
    #source:
    #https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-zip-code-data-soi
    #ex. https://www.irs.gov/statistics/soi-tax-stats-individual-income-tax-statistics-2015-zip-code-data-soi
    #documentation:
    '''
    here's the key to the columns:
    N1, num R: Number of returns
    A00100, AGI: Adjust gross income
    A00200, SW: Salaries and wages amount
    A00300, TI: Taxable interest amount
    A00600, OD: Ordinary dividends amount
    A00650, QD: Qualified dividends amount
    A00900, BPI: Business or professional net income
    A01000, CG: Net capital gain 
    A01400, IRAD: Taxable individual retirement arrangements distributions amount
    A01700, TPA: Taxable pensions and annuities amount
    A02500, TSSB: Taxable Social Security benefits amount
    A03300, SRP: Self-employment retirement plans amount
    '''
    print('Updated by: IRS')
    years = [2009, 2010, 2011, 2012, 2013, 2014, 2015]
    combined = pd.DataFrame()
    for year in years:
        df = pd.read_csv('Data/{}zpallnoagi.csv'.format(str(year)[-2:]))
        df.rename(columns = {'N1': 'num R', 'A00100': 'AGI', 'A00200': 'SW', 'A00300': 'TI', 'A00600': 'OD',
                             'A00650': 'QD', 'A00900': 'BPI', 'A01000': 'CG', 'A01400': 'IRAD', 'A01700': 'TPA',
                             'A02500': 'TSSB', 'A03300': 'SRP'}, inplace = True)
        
        df['avg AGI'] = df['AGI'] * 1000 / df['num R']
        df['avg SW'] = df['SW'] * 1000 / df['num R']
        df['avg OI'] = (df['TI'] + df['OD'] + df['QD'] + df['BPI'] + df['CG'] + df['IRAD'] + df['TPA'] + df['TSSB'] + df['SRP']) * 1000 / df['num R']
        df['ZIPCODE'] = df['ZIPCODE'].apply(lambda x: str(x).zfill(5))
        df['year'] = year
        combined = pd.concat([combined, df[['STATE', 'ZIPCODE', 'year', 'num R', 'avg AGI', 'avg SW', 'avg OI']]], axis = 0)
    
    zip_summary = combined[combined['ZIPCODE'] != '00000']
    state_summary = combined[combined['ZIPCODE'] == '00000']

    zip_summary['num R z'] = (zip_summary['num R'] - zip_summary[zip_summary['year'] == max(years)]['num R'].mean()) / zip_summary[zip_summary['year'] == max(years)]['num R'].std()
    zip_summary['avg AGI z'] = (zip_summary['avg AGI'] - zip_summary[zip_summary['year'] == max(years)]['avg AGI'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg AGI'].std()
    zip_summary['avg SW z'] = (zip_summary['avg SW'] - zip_summary[zip_summary['year'] == max(years)]['avg SW'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg SW'].std()
    zip_summary['avg OI z'] = (zip_summary['avg OI'] - zip_summary[zip_summary['year'] == max(years)]['avg OI'].mean()) / zip_summary[zip_summary['year'] == max(years)]['avg OI'].std()

    zip_summary.sort_values(by = ['ZIPCODE', 'year'], ascending = True, inplace = True)
    zip_summary['num R pchg'] = zip_summary.groupby('ZIPCODE')['num R'].pct_change()
    zip_summary['avg AGI pchg'] = zip_summary.groupby('ZIPCODE')['avg AGI'].pct_change()
    zip_summary['avg OI pchg'] = zip_summary.groupby('ZIPCODE')['avg OI'].pct_change()
    zip_summary['avg SW pchg'] = zip_summary.groupby('ZIPCODE')['avg SW'].pct_change()

    #print(zip_summary[zip_summary['year'] == max(years)]['num R'].mean()) #5230.58227574
    #print(zip_summary[zip_summary['year'] == max(years)]['num R'].std()) #7653.10957042
    #print(zip_summary[zip_summary['year'] == max(years)]['avg AGI'].mean()) #60371.7195512
    #print(zip_summary[zip_summary['year'] == max(years)]['avg AGI'].std()) #46916.1316414
    #print(zip_summary[zip_summary['year'] == max(years)]['avg SW'].mean()) #39698.9126721
    #print(zip_summary[zip_summary['year'] == max(years)]['avg SW'].std()) #20053.2054906
    #print(zip_summary[zip_summary['year'] == max(years)]['avg OI'].mean()) #14984.2323585
    #print(zip_summary[zip_summary['year'] == max(years)]['avg OI'].std()) #25297.6725091
    #print(zip_summary.head())

    state_summary['num R z'] = (state_summary['num R'] - state_summary[state_summary['year'] == max(years)]['num R'].mean()) / state_summary[state_summary['year'] == max(years)]['num R'].std()
    state_summary['avg AGI z'] = (state_summary['avg AGI'] - state_summary[state_summary['year'] == max(years)]['avg AGI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg AGI'].std()
    state_summary['avg SW z'] = (state_summary['avg SW'] - state_summary[state_summary['year'] == max(years)]['avg SW'].mean()) / state_summary[state_summary['year'] == max(years)]['avg SW'].std()
    state_summary['avg OI z'] = (state_summary['avg OI'] - state_summary[state_summary['year'] == max(years)]['avg OI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg OI'].std()

    state_summary.sort_values(by = ['STATE', 'year'], ascending = True, inplace = True)
    state_summary['num R pchg'] = state_summary.groupby('STATE')['num R'].pct_change()
    state_summary['avg AGI pchg'] = state_summary.groupby('STATE')['avg AGI'].pct_change()
    state_summary['avg SW pchg'] = state_summary.groupby('STATE')['avg SW'].pct_change()
    state_summary['avg OI pchg'] = state_summary.groupby('STATE')['avg OI'].pct_change()

    #print(state_summary[state_summary['year'] == max(years)]['num R'].mean()) #2843189.41176
    #print(state_summary[state_summary['year'] == max(years)]['num R'].std()) #3206548.39604
    #print(state_summary[state_summary['year'] == max(years)]['avg AGI'].mean()) #64480.6512188
    #print(state_summary[state_summary['year'] == max(years)]['avg AGI'].std()) #10453.8490415
    #print(state_summary[state_summary['year'] == max(years)]['avg SW'].mean()) #43969.0916724
    #print(state_summary[state_summary['year'] == max(years)]['avg SW'].std()) #6874.7433363
    #print(state_summary[state_summary['year'] == max(years)]['avg OI'].mean()) #16280.5029483
    #print(state_summary[state_summary['year'] == max(years)]['avg OI'].std()) #3525.44202334
    #print(state_summary.head())
    
    if draw == True:
        draw_df = state_summary[['STATE','year','avg AGI']].pivot(index = 'year', columns='STATE', values='avg AGI')
    
        #show top and bottom five states
        n_states = list(draw_df[draw_df.index == max(draw_df.index)].transpose().nsmallest(5, max(draw_df.index)).index) + list(draw_df[draw_df.index == max(draw_df.index)].transpose().nlargest(5, max(draw_df.index)).index)
        draw_df[n_states].plot()
        plt.title('5 States with highest/lowest avg AGI')
        plt.show()
        #show states with largest change
        n_states = list(((draw_df.iloc[-1] - draw_df.iloc[0]) / draw_df.iloc[0] * 100).nlargest(5).index)
        draw_df[n_states].plot()
        plt.title('5 States with largest % change in avg AGI')
        plt.show()
    return zip_summary, state_summary

#get zillow data
    
if __name__ == '__main__':
    #fed_rate, fed_rate_annual = get_fed_fund(True)
    #mortgage_rate, mortgage_rate_annual = get_mtg_rate(True)
    #market, market_annual = get_market(True)
    #census_data = get_census()
    #irs_zip_data, irs_state_data = get_irs(True)
