#! /usr/bin/python3

import sqlite3
import pandas as pd

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'


# Calculate station days
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql = """SELECT DISTINCT startstationid AS stationid,startdate AS date FROM trip WHERE startweekday NOT IN ('Saturday','Sunday')"""
start = pd.read_sql(sql, conn)
sql = """SELECT DISTINCT endstationid AS stationid,enddate AS date FROM trip WHERE endweekday NOT IN ('Saturday','Sunday')"""
end = pd.read_sql(sql, conn)
wkday = pd.concat([start, end])
wkday = wkday.drop_duplicates()
wkday = wkday.groupby(['stationid'], as_index=False)['date'].count()
wkday.columns = ['stationid', 'weekdays']
sql = """SELECT DISTINCT startstationid AS stationid,startdate AS date FROM trip WHERE startweekday IN ('Saturday','Sunday')"""
start = pd.read_sql(sql, conn)
sql = """SELECT DISTINCT endstationid AS stationid,enddate AS date FROM trip WHERE endweekday IN ('Saturday','Sunday')"""
end = pd.read_sql(sql, conn)
wkend = pd.concat([start, end])
wkend = wkend.drop_duplicates()
wkend = wkend.groupby(['stationid'], as_index=False)['date'].count()
wkend.columns = ['stationid', 'weekends'] 
df = pd.merge(wkday, wkend, how='outer', on='stationid')
df.to_csv(path + 'stationdays.csv', index=False, na_rep=0)

conn.close()


# Merge station list and station days
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

station = pd.read_csv(path + 'station.csv')
wkd = pd.read_csv(path + 'stationdays.csv')
station=station[['stationid','stationname','stationlat','stationlong']]
df = pd.merge(station, wkd, how='outer', on='stationid').sort_values('stationid')
df.to_sql('station', conn, if_exists='replace', index=False)

conn.close()