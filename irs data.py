import pandas as pd
'''
IRS_dict = {}
years = ['09', '10', '11', '12', '13', '14']
for year in years:
    df = pd.read_csv('E:\WinUser\Documents\Python Code\Zillow Fun\{}zpallnoagi.csv'.format(year))
    df['avg AGI'] = df['A00100'] * 1000 / df['N1']
    col1 = 'num_R' + year
    col2 = 'AGI' + year
    df.rename(columns = {'N1': col1, 'A00100': col2}, inplace = True)
    IRS_dict[year] = df[['STATE', 'ZIPCODE', col1, col2]]


print(IRS_dict['10'].head())


combined = pd.DataFrame()
for year in years:
    if combined.shape == (0, 0):
        combined = IRS_dict[year]
    else:
        combined.merge(IRS_dict[year], how = 'outer',
                       left_on = ['STATE', 'ZIPCODE'],
                       right_on = ['STATE', 'ZIPCODE'],
                       suffixes = ()

'''
years = [2009, 2010, 2011, 2012, 2013, 2014]
combined = pd.DataFrame()
for year in years:
    df = pd.read_csv('E:\WinUser\Documents\Python Code\Zillow Fun\{}zpallnoagi.csv'.format(str(year)[-2:]))
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

print(zip_summary[zip_summary['year'] == max(years)]['num R'].mean()) #5230.58227574
print(zip_summary[zip_summary['year'] == max(years)]['num R'].std()) #7653.10957042
print(zip_summary[zip_summary['year'] == max(years)]['avg AGI'].mean()) #60371.7195512
print(zip_summary[zip_summary['year'] == max(years)]['avg AGI'].std()) #46916.1316414
print(zip_summary[zip_summary['year'] == max(years)]['avg SW'].mean()) #39698.9126721
print(zip_summary[zip_summary['year'] == max(years)]['avg SW'].std()) #20053.2054906
print(zip_summary[zip_summary['year'] == max(years)]['avg OI'].mean()) #14984.2323585
print(zip_summary[zip_summary['year'] == max(years)]['avg OI'].std()) #25297.6725091
print(zip_summary.head())

state_summary['num R z'] = (state_summary['num R'] - state_summary[state_summary['year'] == max(years)]['num R'].mean()) / state_summary[state_summary['year'] == max(years)]['num R'].std()
state_summary['avg AGI z'] = (state_summary['avg AGI'] - state_summary[state_summary['year'] == max(years)]['avg AGI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg AGI'].std()
state_summary['avg SW z'] = (state_summary['avg SW'] - state_summary[state_summary['year'] == max(years)]['avg SW'].mean()) / state_summary[state_summary['year'] == max(years)]['avg SW'].std()
state_summary['avg OI z'] = (state_summary['avg OI'] - state_summary[state_summary['year'] == max(years)]['avg OI'].mean()) / state_summary[state_summary['year'] == max(years)]['avg OI'].std()

state_summary.sort_values(by = ['STATE', 'year'], ascending = True, inplace = True)
state_summary['num R pchg'] = state_summary.groupby('STATE')['num R'].pct_change()
state_summary['avg AGI pchg'] = state_summary.groupby('STATE')['avg AGI'].pct_change()
state_summary['avg SW pchg'] = state_summary.groupby('STATE')['avg SW'].pct_change()
state_summary['avg OI pchg'] = state_summary.groupby('STATE')['avg OI'].pct_change()

print(state_summary[state_summary['year'] == max(years)]['num R'].mean()) #2843189.41176
print(state_summary[state_summary['year'] == max(years)]['num R'].std()) #3206548.39604
print(state_summary[state_summary['year'] == max(years)]['avg AGI'].mean()) #64480.6512188
print(state_summary[state_summary['year'] == max(years)]['avg AGI'].std()) #10453.8490415
print(state_summary[state_summary['year'] == max(years)]['avg SW'].mean()) #43969.0916724
print(state_summary[state_summary['year'] == max(years)]['avg SW'].std()) #6874.7433363
print(state_summary[state_summary['year'] == max(years)]['avg OI'].mean()) #16280.5029483
print(state_summary[state_summary['year'] == max(years)]['avg OI'].std()) #3525.44202334
print(state_summary.head())

#zip_summary.to_csv('irs_zip_data.csv', index = False)
#state_summary.to_csv('irs_state_data.csv', index = False)
