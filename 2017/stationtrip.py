#! /usr/bin/python3

import sqlite3
import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'


# Calculate average weekday and weekend trips
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)
sql = """SELECT startstationid AS stationid,COUNT(*) AS startweekdaytrip FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') """
sql += """GROUP BY startstationid"""
startwkday = pd.read_sql(sql, conn)
sql = """SELECT endstationid AS stationid,COUNT(*) AS endweekdaytrip FROM trip WHERE endweekday NOT IN ('Saturday','Sunday') """
sql += """GROUP BY endstationid"""
endwkday = pd.read_sql(sql, conn)
df = pd.merge(station, startwkday, how='outer', on='stationid').sort_values('stationid')
df = pd.merge(df, endwkday, how='outer', on='stationid').sort_values('stationid')
df['startweekdaytrip'] = np.where(pd.isna(df['startweekdaytrip']), 0, df['startweekdaytrip'].astype(float))
df['endweekdaytrip'] = np.where(pd.isna(df['endweekdaytrip']), 0, df['endweekdaytrip'].astype(float))
df['totalweekdaytrip'] = df['startweekdaytrip'] + df['endweekdaytrip']
df['avgweekdaytrip'] = np.where(df['weekdays'] == 0, 0, df['totalweekdaytrip'] / df['weekdays'])

sql = """SELECT startstationid AS stationid,COUNT(*) AS startweekendtrip FROM trip WHERE startweekday IN ('Saturday','Sunday') """
sql += """GROUP BY startstationid"""
startwkend = pd.read_sql(sql, conn)
sql = """SELECT endstationid AS stationid,COUNT(*) AS endweekendtrip FROM trip WHERE endweekday IN ('Saturday','Sunday') """
sql += """GROUP BY endstationid"""
endwkend = pd.read_sql(sql, conn)
df = pd.merge(df, startwkend, how='outer', on='stationid').sort_values('stationid')
df = pd.merge(df, endwkend, how='outer', on='stationid').sort_values('stationid')
df['startweekendtrip'] = np.where(pd.isna(df['startweekendtrip']), 0, df['startweekendtrip'].astype(float))
df['endweekendtrip'] = np.where(pd.isna(df['endweekendtrip']), 0, df['endweekendtrip'].astype(float))
df['totalweekendtrip'] = df['startweekendtrip'] + df['endweekendtrip']
df['avgweekendtrip'] = np.where(df['weekends'] == 0, 0, df['totalweekendtrip'] / df['weekends'])

df.to_csv(path + 'stationtrip.csv', index=False)
df.to_sql('stationtrip',conn,if_exists='replace',index=False)

conn.close()
