#! /usr/bin/python3

import sqlite3
import pandas as pd

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'


# Calculate hourly distribution
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)
station['stationid']=station['stationid'].astype(object)

sql = """SELECT startstationid AS stationid,starthour AS hour,count(*) as weekdaystarttrip FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,starthour"""
start = pd.read_sql(sql, conn)
start = start.pivot(index='stationid', columns='hour', values='weekdaystarttrip')
start['total'] = start.sum(axis=1)
start.loc['total', :] = start.sum(axis=0)
for i in range(0, 24):
    start.loc[:, i] = start.loc[:, i] / start.loc[:, 'total']
start.columns = ['weekdaystart' + str(x) for x in start.columns]
start = start.reset_index()

df = pd.merge(station, start, how='outer', on='stationid')

sql = """SELECT endstationid AS stationid,endhour AS hour,count(*) as weekdayendtrip FROM trip WHERE endweekday NOT IN ('Saturday','Sunday') """
sql += """ GROUP BY endstationid,endhour"""
end = pd.read_sql(sql, conn)
end = end.pivot(index='stationid', columns='hour', values='weekdayendtrip')
end['total'] = end.sum(axis=1)
end.loc['total', :] = end.sum(axis=0)
for i in range(0, 24):
    end.loc[:, i] = end.loc[:, i] / end.loc[:, 'total']
end.columns = ['weekdayend' + str(x) for x in end.columns]
end = end.reset_index()

df = pd.merge(df, end, how='outer', on='stationid')

sql = """SELECT startstationid AS stationid,starthour AS hour,count(*) as weekendstarttrip FROM trip WHERE startweekday IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,starthour"""
start = pd.read_sql(sql, conn)
start = start.pivot(index='stationid', columns='hour', values='weekendstarttrip')
start['total'] = start.sum(axis=1)
start.loc['total', :] = start.sum(axis=0)
for i in range(0, 24):
    start.loc[:, i] = start.loc[:, i] / start.loc[:, 'total']
start.columns = ['weekendstart' + str(x) for x in start.columns]
start = start.reset_index()

df = pd.merge(df, start, how='outer', on='stationid')

sql = """SELECT endstationid AS stationid,endhour AS hour,count(*) as weekendendtrip FROM trip WHERE endweekday IN ('Saturday','Sunday') """
sql += """ GROUP BY endstationid,endhour"""
end = pd.read_sql(sql, conn)
end = end.pivot(index='stationid', columns='hour', values='weekendendtrip')
end['total'] = end.sum(axis=1)
end.loc['total', :] = end.sum(axis=0)
for i in range(0, 24):
    end.loc[:, i] = end.loc[:, i] / end.loc[:, 'total']
end.columns = ['weekendend' + str(x) for x in end.columns]
end = end.reset_index()

df = pd.merge(df, end, how='outer', on='stationid')

df.to_csv(path + 'stationhourly.csv', index=False, na_rep=0)
df=pd.read_csv(path+'stationhourly.csv')
df.to_sql('stationhourly',conn,if_exists='replace',index=False)

conn.close()