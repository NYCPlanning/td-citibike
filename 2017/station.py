#! /usr/bin/python3

import sqlite3
import pandas as pd

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'


# Create station list
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql = """SELECT startstationid AS stationid,startstationname AS stationname,startstationlat AS stationlat,startstationlong AS stationlong,COUNT(*) AS count """
sql += """FROM trip GROUP BY startstationid,startstationname,startstationlat,startstationlong"""
start = pd.read_sql(sql, conn)
start['stationlat']=round(start['stationlat'].astype(float),6)
start['stationlong']=round(start['stationlong'].astype(float),6)
sql = """SELECT endstationid AS stationid,endstationname AS stationname,endstationlat AS stationlat,endstationlong AS stationlong,COUNT(*) AS count """
sql += """FROM trip GROUP BY endstationid,endstationname,endstationlat,endstationlong"""
end = pd.read_sql(sql, conn)
end['stationlat']=round(end['stationlat'].astype(float),6)
end['stationlong']=round(end['stationlong'].astype(float),6)
df = pd.concat([start, end])
df = df.groupby(by=['stationid', 'stationname', 'stationlat', 'stationlong'], as_index=False)['count'].sum()
df = df.sort_values('stationid')
# df.to_csv(path + 'station.csv', index=False)  # Manually remove duplicates

conn.close()