#! /usr/bin/python3

import sqlite3
import pandas as pd
import numpy as np


pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'
path='D:/CITIBIKE2018/'


# Import raw data into SQLite database
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

for i in range(1, 13):
    tp = pd.read_csv(path + 'RAW/2018' + str(i).zfill(2) + '-citibike-tripdata.csv', dtype=object)
    tp.columns = ['tripduration', 'starttime', 'endtime', 'startstationid', 'startstationname', 'startstationlat',
                  'startstationlong', 'endstationid', 'endstationname', 'endstationlat', 'endstationlong', 'bikeid',
                  'usertype', 'birthyear', 'gender']
    tp['startstationid']=['ST'+x.zfill(4) for x in tp['startstationid']]
    tp['endstationid']=['ST'+x.zfill(4) for x in tp['endstationid']]
    jc = pd.read_csv(path + 'RAW/JC-2017' + str(i).zfill(2) + '-citibike-tripdata.csv', dtype=object)
    jc.columns = ['tripduration', 'starttime', 'endtime', 'startstationid', 'startstationname', 'startstationlat',
                  'startstationlong', 'endstationid', 'endstationname', 'endstationlat', 'endstationlong', 'bikeid',
                  'usertype', 'birthyear', 'gender']
    jc['startstationid']=['ST'+x.zfill(4) for x in jc['startstationid']]
    jc['endstationid']=['ST'+x.zfill(4) for x in jc['endstationid']]   
    df = tp.append(jc)
    df = df.sort_values(by=['starttime', 'startstationid'], axis=0, ascending=True)
    df['tripid'] = ['TP2017' + str(i).zfill(2) + y for y in [str(x).zfill(7) for x in range(1, max(df.count()) + 1)]]
    df['startdate'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S').dt.date
    df['startmonth'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S').dt.month
    df['startweekday'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S').dt.weekday_name
    df['starthour'] = pd.to_datetime(df['starttime'], format='%Y-%m-%d %H:%M:%S').dt.hour
    df['startstationlat'] = df['startstationlat'].astype(float)
    df['startstationlong'] = df['startstationlong'].astype(float)
    df['enddate'] = pd.to_datetime(df['endtime'], format='%Y-%m-%d %H:%M:%S').dt.date
    df['endmonth'] = pd.to_datetime(df['endtime'], format='%Y-%m-%d %H:%M:%S').dt.month
    df['endweekday'] = pd.to_datetime(df['endtime'], format='%Y-%m-%d %H:%M:%S').dt.weekday_name
    df['endhour'] = pd.to_datetime(df['endtime'], format='%Y-%m-%d %H:%M:%S').dt.hour
    df['endstationlat'] = df['endstationlat'].astype(float)
    df['endstationlong'] = df['endstationlong'].astype(float)
    df['odid']=['OD'+x+y for x,y in zip(df['startstationid'],df['endstationid'])]
    df['tripduration'] = df['tripduration'].astype(float)
    df['bikeid'] = ['BK'+x.zfill(5) for x in df['bikeid']]
    df['birthyear'] = np.where(pd.isna(df['birthyear']), np.nan, df['birthyear'].astype(float))
    df['age'] = np.where(pd.isna(df['birthyear']), np.nan, 2017 - df['birthyear'])
    df['gender'] = np.where(df['gender'] == '1', 'Male', np.where(df['gender'] == '2', 'Female', 'Unknown'))
    df = df[['tripid', 'starttime', 'startdate', 'startmonth', 'startweekday', 'starthour', 'startstationid', 'startstationname',
             'startstationlat', 'startstationlong', 'endtime', 'enddate', 'endmonth', 'endweekday', 'endhour', 'endstationid',
             'endstationname', 'endstationlat', 'endstationlong', 'odid', 'tripduration', 'bikeid', 'usertype', 'birthyear',
             'age', 'gender']]
    df.to_sql('trip', conn, if_exists='append', index=False)

conn.close()