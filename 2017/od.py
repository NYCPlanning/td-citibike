#! /usr/bin/python3

import sqlite3
import pandas as pd

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'


# origin-destination
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekdaytrip """
sql += """ FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
wkday = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekendtrip """
sql += """ FROM trip WHERE startweekday IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
wkend = pd.read_sql(sql, conn)

df=pd.merge(wkday,wkend,how='outer',on=['stationid1','stationid2'])
df=pd.merge(df,station,how='left',left_on='stationid1',right_on='stationid')
df=pd.merge(df,station,how='left',left_on='stationid2',right_on='stationid')
df['weekdays']=[min(x,y) for x,y in zip(df['weekdays_x'],df['weekdays_y'])]
df['weekends']=[min(x,y) for x,y in zip(df['weekends_x'],df['weekends_y'])]

df=df[['stationid1','stationname_x','stationlat_x','stationlong_x','stationid2','stationname_y','stationlat_y','stationlong_y',
       'weekdays','weekends','totalweekdaytrip','totalweekendtrip']]
df.columns=['stationid1','stationname1','stationlat1','stationlong1','stationid2','stationname2','stationlat2','stationlong2',
            'weekdays','weekends','totalweekdaytrip','totalweekendtrip']
df['avgweekdaytrip']=df['totalweekdaytrip']/df['weekdays']
df['avgweekendtrip']=df['totalweekendtrip']/df['weekends']

df.to_csv(path + 'od.csv', index=False, na_rep=0)
df=pd.read_csv(path+'od.csv')
df.to_sql('od',conn,if_exists='replace',index=False)

conn.close()