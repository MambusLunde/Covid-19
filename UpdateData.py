# Importing modules
###############################
import datetime
import numpy as np
import pandas as pd

def Dataset(url1, url2, local1, final_loc):
    """
    A function to import, transform, and return the data needed for 
    the visualizations.
    
    Args:
        url1(string): A url to a dataset.
        url2(string): A url to a dataset.
        local1(string): A path to a local file.
        final_loc(string): Where to save the file.
    
    Returns: 
        final_df(DataFrame): DataFrame of data.
    """
    
    # Loading the various datasets
    confirmed   = get_data(url1)
    deaths      = get_data(url2)
    population  = get_data(local1)
    
    # Making sure that both timeseries are updated by comparing the latest dates
    assert [*confirmed.columns][-1] == [*deaths.columns][-1], 'Timeseries do not match.'
    
    # Fixing/adding FIPS columns
    confirmed   = fix_fips(confirmed)
    deaths      = fix_fips(deaths)
    population  = fix_fips(population)
    
    # Setting index of population
    population = population.set_index('FIPS')
    
    # Generating dicitonaries
    list_dict, state_dict = get_dicts(confirmed, population)
    
    final_df = combine_data(confirmed, deaths, population, list_dict, state_dict)
    
    final_df['New Cases'] = new_column(column = 'Confirmed', 
                                       kind = 'average', 
                                       data = final_df,
                                       data2 = population,
                                       dict1 = list_dict,
                                       days=1)
    
    final_df['New Deaths'] = new_column(column = 'Deaths', 
                                        kind = 'average',  
                                        data = final_df,
                                        data2 = population,
                                        dict1 = list_dict,
                                        days = 1)
    
    final_df['Cases, 7DMA'] = new_column(column = 'Confirmed', 
                                         kind = 'average', 
                                         data = final_df,
                                         data2 = population,
                                         dict1 = list_dict, 
                                         days = 7)
    
    final_df['Deaths, 7DMA'] = new_column(column = 'Deaths',
                                          kind = 'average', 
                                          data = final_df,
                                          data2 = population,
                                          dict1 = list_dict,
                                          days = 7)
    
    final_df['New Cases Per 100k, 7DMA'] = new_column(column = 'Cases, 7DMA',
                                                      kind = 'PER100K', 
                                                      data = final_df,
                                                      data2 = population,
                                                      dict1 = list_dict
                                                     )

    final_df['Deaths Per 100k'] = new_column(column = 'Deaths',
                                                       kind = 'PER100K', 
                                                       data = final_df,
                                                       data2 = population,
                                                       dict1 = list_dict
                                                      )    
    
    # Removing decimals
    final_df = final_df.astype('int')
    
    final_df.reset_index(inplace = True)

    final_df.to_csv(final_loc, index = False)
    
    return final_df, list_dict
    
def get_data(location):
    """
    Import data from the given location.
    
    Args: 
        location(string): Where to import the data from.
                
    Returns:
        df(DataFrame): DataFrame of the data at the given location.
    """
    
    # Importing data to a pandas DataFrame
    df = pd.read_csv(location)
    
    # Returning dataframe
    return df

def fix_fips(df):
    """
    Formatting FIPS values.
    
    Returns:
        df(DataFrame): Updated dataframe.
    """
    

    
    # Making sure that the FIPS values are strings, and zero-padded.
    df['FIPS'] = df['FIPS'].fillna(0).astype('int32').astype('str').str.zfill(5)

    
    # Returning the new dataframe
    return df

def get_dicts(df1, df2):
    """
    Generates various dicts, to make things easier and more readable.
    
    Returns:
        list_dict(dict): Dictionary of lists.
    """
    
    # Making a list of all FIPS values in the population set (States and counties)
    pop_fips = [*df2.index]
    
    df1 = df1.sort_values('FIPS')
    # Making a list of county fips values, these are between '01001' and '56045' (included)
    index_01001 = int(*df1[df1['FIPS']=="01001"].index)
    index_56045 = int(*df1[df1['FIPS']=="56045"].index)
    county_fips = list(df1.loc[index_01001:index_56045,'FIPS'])
    
    # Making a list of state fips, this is the difference between the two prior lists
    # Sorting because sets are unsorted
    state_fips = sorted(list(set(pop_fips) - set(county_fips)))
    
    # Adding a fips value for the US total '00000' to the pop_fips
    all_fips = ['00000']
    all_fips.extend(pop_fips)
    all_fips = sorted(all_fips)
    
    # Making a list of dates found in the timeseries
    dates = [*df1.iloc[:,11:].columns]
    
    # Adding the lists to a dictionary
    list_dict = {}
    
    list_dict['all_fips'] = all_fips
    list_dict['state_fips'] = state_fips
    list_dict['county_fips'] = county_fips
    list_dict['pop_fips'] = pop_fips
    list_dict['dates'] = dates
    
    # Making a dict of state names
    state_dict = {fips: df2.loc[fips,'STNAME'] for fips in state_fips}

    # Returning the dictionaries
    return list_dict, state_dict

def combine_data(df1,df2,df3,dict1,dict2):
    """
    Making a new dataframe to use for vizualizations. 
    
    Returns:
        df(DataFrame): A Multiindex DataFrame with FIPS and date as indexes. 
    """
    # Creating dictionaries for counties, state totals, and overall totals
    # Where the keys are tuples of the fips and the date
  
    usa_total = {('00000', date): [np.sum(df1[date].values),np.sum(df2[date].values)] for date in dict1['dates']}

    
    # This part is basically just for me to quickly see how many new cases and deaths in the last day
    snapshot = usa_total[('00000',dict1['dates'][-2])]
    snap = usa_total[('00000',dict1['dates'][-1])]
    s = np.array(snap)-np.array(snapshot)
    print(f"There were {s[0]} new cases and {s[1]} new deaths on {dict1['dates'][-1]}.")
    
    counties_dict = {(fips, date): [df1.loc[int(*df1[df1['FIPS'] == fips].index),date],\
                                    df2.loc[int(*df2[df2['FIPS'] == fips].index),date]]\
                     for fips in dict1['county_fips'] for date in dict1['dates'] } 
    
    
    states = {(fips, date): [np.sum(df1[df1['Province_State'] == dict2[fips]][date]),\
                             np.sum(df2[df2['Province_State'] == dict2[fips]][date])]\
              for fips in dict1['state_fips'] for date in dict1['dates']}
    
    
    # Transforming to DataFrames, and transposing to have the wanted structure
    df = pd.DataFrame(counties_dict).transpose()
    usa_df = pd.DataFrame(usa_total).transpose()
    states_df = pd.DataFrame(states).transpose()
    
    # Naming columns and indexes
    columns = ['Confirmed','Deaths']
    index = ['FIPS','Date']
    
    # Stacking the DataFrames on top of each other
    final_df = pd.concat([df,usa_df,states_df],axis=0)
    
    # Naming columns and indexes
    final_df.columns = columns
    final_df.index.names = index
    
    # Popping the index out to make the dates into datetime and re-setting index
    final_df = final_df.reset_index()
    final_df['Date']=pd.to_datetime(final_df['Date'],format='%m/%d/%y')
    final_df = final_df.set_index(index)
    
    # Sorting the dataframe, which is why the dates had to be datetime!
    final_df = final_df.sort_index()
    
    # Returning the dataframe that will be added to
    return final_df

def new_column(column, kind, data, data2, dict1, days = 1):
    """
    Generates a list of new values to be added as a new column.
    
    Args:
        column(string): Column to do calculations on.
        data(df): dataframe that holds the data
        data2(df): population data
        dict1(dictionary): dict with different lists
        kind(string): kwarg to decide what type of calculation.
            'average': average between dates in the timeseries. Number of days given by days.
            'capita': per 100k population.
        days(int): Number of days 
    
    Returns:
        lst(list): list of values to be the new column.
    """
    lst = []
    # Checking if we're doing a difference calculation
    if kind == 'average':
        
        # Looping over all fips values
        for fips in dict1['all_fips']:
            
            # Taking the difference between days, and then dividing by that number of days
            temp = round(data.loc[fips, column].diff(periods=days).fillna(0)/days,0)
            
            # For each FIPS value add to the list
            lst.extend(temp)
            
        # Returning the list
        return lst
        
    # Checking if we're doing a per 100k calculation
    elif kind == 'PER100K':
        
        # First finding the overall population (scaled)
        total = 0
        
        for fips in dict1['state_fips']:
            total = total + data2.loc[fips,kind]
            
        # A special case for '00000'
        temp = round(data.loc['00000',column] / total, 0)
        lst.extend(temp)
        
        # For the rest of the FIPS
        for fips in dict1['pop_fips']:
            
            temp = round(data.loc[fips,column] / data2.loc[fips,kind],0)
            lst.extend(temp)
            
        return lst

# Timeseries of confirmed cases in the US
url1 = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv"

# Timeseries of confirmed deathes in the US
url2 = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv"

# The modified population estimate
local1 = 'data/population.csv'

final_loc = 'data/us_covid.csv'
us_covid, list_dict = Dataset(url1, url2, local1, final_loc) 


# Saving a csv to use with PowerBI
power = us_covid[us_covid['Date']==max(us_covid['Date'])]
power = power.set_index('FIPS')
lst = list_dict['county_fips']
power = power.loc[lst]
power.reset_index(inplace = True)
power.to_csv('data/choropleth.csv') 