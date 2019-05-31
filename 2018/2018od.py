# -*- coding: utf-8 -*-
"""
Created on Wed May 29 16:47:26 2019

@author: F_Du
"""
import sqlite3
import pandas as pd
import numpy as np

from datetime import datetime
start=datetime.now()

pd.set_option('display.max_columns', 500)
path = 'C:/Users/F_Du/Desktop/Citi Bike/CitiBikeGH-master/CitiBikeGH-master/citibikedata/2018/'




# origin-destination
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekdaytrip """
sql += """ FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
wkday = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekendtrip """
sql += """ FROM trip WHERE startweekday IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
wkend = pd.read_sql(sql, conn)


df=pd.merge(wkday,wkend,how='outer',on=['stationid1','stationid2'])


df['totalweekdaytrip'] = np.where(np.isnan(df['totalweekdaytrip']), 0, df['totalweekdaytrip'].astype(float))
df['totalweekendtrip'] = np.where(np.isnan(df['totalweekendtrip']), 0, df['totalweekendtrip'].astype(float))

### weekdays 


sql = """SELECT DISTINCT startstationid AS stationid,startdate AS startdate FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,startdate """
uniquetablestartweekday = pd.read_sql(sql, conn)
sql = """SELECT DISTINCT endstationid AS stationid,startdate AS startdate FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,startdate """
uniquetableendweekday = pd.read_sql(sql, conn)
uniquetableweekday = pd.concat([uniquetablestartweekday, uniquetableendweekday])
uniquetableweekday = uniquetableweekday.drop_duplicates()

uniquetableweekday.head()

#weekend
sql = """SELECT DISTINCT startstationid AS stationid,startdate AS startdate FROM trip WHERE startweekday IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,startdate """
uniquetablestartweekendday = pd.read_sql(sql, conn)
sql = """SELECT DISTINCT endstationid AS stationid,startdate AS startdate FROM trip WHERE startweekday IN ('Saturday','Sunday') """
sql += """ GROUP BY startstationid,startdate """
uniquetableendweekendday = pd.read_sql(sql, conn)
uniquetableweekendday = pd.concat([uniquetablestartweekendday, uniquetableendweekendday])
uniquetableweekendday = uniquetableweekendday.drop_duplicates()

uniquetableweekendday.head()



df['weekdays']=0
df['weekends']=0




for i in range(0,len(df)):
    origin= df['stationid1'][i]
    destination = df['stationid2'][i]
    originweekdates = uniquetableweekday[ uniquetableweekday['stationid'] ==origin]
    destinationweekdates =  uniquetableweekday[ uniquetableweekday['stationid'] ==destination]
    weekday =len(set(originweekdates['startdate']).intersection(set(destinationweekdates['startdate'])))
    df['weekdays'][i]= weekday
    originweekenddates = uniquetableweekendday[ uniquetableweekendday['stationid'] ==origin]
    destinationweekenddates =  uniquetableweekendday[ uniquetableweekendday['stationid'] ==destination]
    weekends =len(set(originweekenddates['startdate']).intersection(set(destinationweekenddates['startdate'])))
    df['weekends'][i]= weekends
    print(i)





df = pd.merge(df, station[['stationid','stationname','stationlat','stationlong']], left_on = 'stationid1', right_on = 'stationid', how = 'left')
df = pd.merge(df, station[['stationid','stationname','stationlat','stationlong']], left_on = 'stationid2', right_on = 'stationid', how = 'left')


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

print(datetime.now()-start)

# =============================================================================
# outlier:
#     ST3650
# STATIONNAME1
# 8D Mobile 01
# STATIONLAT1
# 45.506264
# STATIONLONG1
# -73.568906
# =============================================================================


