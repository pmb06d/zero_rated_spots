# -*- coding: utf-8 -*-
"""
Created on Thu Aug 29 16:01:08 2019

@author: pbonnin
"""

import pandas as pd
import time
from tqdm import tqdm

pd.options.display.max_columns = 20

bigfile = pd.read_csv('C:/Users/pbonnin/Desktop/P&G Mexico Analysis.csv')

def get_hour(tb_string):
    return(int(tb_string[:2]))


#list(bigfile['Interval'].unique())
def dict(original,normalized):
    # make sure both arrays are the same size with matching pairs
    temp_dict = pd.DataFrame(list(zip(original,normalized)))
    temp_dict = temp_dict.set_index(0)
    temp_dict = temp_dict.iloc[:,0].to_dict()
    return(temp_dict)

# sequentially replace na values down a list with the string above it (useful for merged rows...)
def replicate_down(raw_list):

  column_names = []

  for i in range(len(raw_list)):
    if str(raw_list[i]) == 'nan':
      if str(raw_list[i-1]) == 'nan' and i != 0:
        column_names.append(column_names[-1])
      else:
        column_names.append(raw_list[i-1])
    else:
      column_names.append(raw_list[i])

  return(column_names)

def get_start(timeband):
  temp_list = timeband.replace(' ','').split('-')
  start_time = temp_list[0][:5]
  return(start_time)

def get_unique_timebands(interval):
    tbs = list(bigfile.loc[bigfile['Interval']==interval,'TimeBand'].unique())
    start_times = [get_start(i) for i in tbs]
    return(pd.DataFrame(list(zip(tbs, start_times)),columns=['TimeBand - '+interval,'Start Time']))
    
minute1 = get_unique_timebands('1 minute(s)')
#minute15 = get_unique_timebands('15 minute(s)')
#minute30 = get_unique_timebands('30 minute(s)')
minute60 = get_unique_timebands('60 minute(s)')


extrapolator = minute1.merge(minute60, how='left', left_on='Start Time', right_on='Start Time')


#for timeband in [ 'TimeBand - 15 minute(s)','TimeBand - 30 minute(s)','TimeBand - 60 minute(s)']:
#    extrapolator[timeband] = replicate_down(list(extrapolator[timeband]))

extrapolator['TimeBand - 60 minute(s)'] = replicate_down(list(extrapolator['TimeBand - 60 minute(s)']))
    
#%%

import itertools
start_time = time.time()

keepers = ['Region',
             'Target',
             'Channel',
             'Date',
             'Rat% {Av(Wg)}',
             'Interval',
             'TimeBand']

alt_keepers = ['Region',
             'Target',
             'Channel',
             'Date',
             'Rat% {Av(Wg)}',
             'Interval']


bigfile2 = bigfile.loc[bigfile['Target'].notna(),keepers]
bigfile2 = bigfile2.loc[(bigfile2['Target']!='Universe')&(bigfile2['Channel']=='Cinemax'),]
bigfile2['Hour'] = bigfile2['TimeBand'].apply(get_hour)


dimensions = pd.DataFrame(list(itertools.product(list(bigfile2['Region'].unique()),
                                                 list(bigfile2['Date'].unique()),
                                                 list(bigfile2['Target'].unique()),
                                                 list(bigfile2['Channel'].unique()),
                                                 list(bigfile2['Interval'].unique()))))


dimensions = dimensions.loc[(dimensions[2]!='Universe')&(dimensions[3]=='Cinemax'),]

print('Time: %s seconds' % (time.time() - start_time),'\n')

#%%

def count_zeroes(temp_df, check_under=0):
    timeband = ['Morning','Afternoon','Day','Primetime','Late']
    timeband_int = [list(range(6,13)),
                    list(range(13,16)),
                    list(range(6,16)),
                    list(range(16,25)),
                    list(range(25,30))]
    temp_list = []
    for array in timeband_int:
        temp_list.append(len(temp_df.loc[(temp_df['Hour'].isin(array))&
                                         (temp_df['Rat% {Av(Wg)}']<=check_under),:]))
    
    return(pd.DataFrame(list(zip(timeband,temp_list)),columns=['TimeBand','Zero-Rating-Mins']))


def assign_timeband(i):
    timeband = ['Morning','Afternoon','Primetime','Late']
    timeband_int = [list(range(6,13)),
                    list(range(13,16)),
                    list(range(16,25)),
                    list(range(25,30))]
    
    lookup = []
    for name, time_range in zip(timeband, timeband_int):
        temp_df = pd.DataFrame(time_range)
        temp_df['TimeBand'] = name
        lookup.append(temp_df)
        
    loookup_df = pd.concat(lookup,sort=False)
    loookup_df.columns = ['Hour','TimeBand']
    
    return(loookup_df.loc[loookup_df['Hour']==i,'TimeBand'].values[0])


#%% Using Swifter? Another multiprocessing package

#import swifter

# tqdm.pandas()
# bigfile2['TimeBand_NM'] = bigfile2['Hour'].progress_apply(assign_timeband)

#bigfile2['TimeBand_NM'] = bigfile2['Hour'].swifter.apply(assign_timeband)


#%%

start_time = time.time()
    
df_list = []

for i in tqdm(range(len(dimensions))):
    region = dimensions.iloc[i,0]
    date = dimensions.iloc[i,1]
    target = dimensions.iloc[i,2]
    channel = dimensions.iloc[i,3]
    interval = dimensions.iloc[i,4]
    
    temp_file = bigfile2.loc[(bigfile2['Region']==region)&
                             (bigfile2['Date']==date)&
                             (bigfile2['Target']==target)&
                             (bigfile2['Channel']==channel)&
                             (bigfile2['Interval']==interval),:]
    
    df_for_list = count_zeroes(temp_file)
    
    df_for_list['Region'] = region
    df_for_list['Date'] = date
    df_for_list['Target'] = target
    df_for_list['Channel'] = channel
    df_for_list['Interval'] = interval
    
    df_list.append(df_for_list)


#print('Time: %s seconds' % (time.time() - start_time),'\n')


#start_time = time.time()

new_df = pd.concat(df_list,sort=False)

intervals = ['60 minute(s)', '1 minute(s)']
int_intervals = [60,1]

for label, interval in zip(intervals,int_intervals):
    new_df.loc[new_df['Interval']==label,'Zero-Rating-Mins'] = new_df.loc[new_df['Interval']==label,'Zero-Rating-Mins'].apply(lambda x: x*interval)

output = 'C:/Users/pbonnin/Desktop/P&G Mexico Analysis (Cinemax Only - Processed).csv'
new_df.to_csv(path_or_buf=output, sep=',', index=False)


print('Time: %s seconds' % (time.time() - start_time),'\n')

