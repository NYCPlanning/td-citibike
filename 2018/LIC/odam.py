# -*- coding: utf-8 -*-
"""
Created on Mon Jun  3 19:28:24 2019

@author: F_Du
"""




import sqlite3
import pandas as pd
import numpy as np

from datetime import datetime
start=datetime.now()

pd.set_option('display.max_columns', 500)
path = 'C:/Users/F_Du/Desktop/Citi Bike/CitiBikeGH-master/CitiBikeGH-master/citibikedata/2018/'


# LIC AM PEAK 8AM origin-destination
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)

licstationid = pd.read_csv(path+'LICstationid.csv', dtype=object)
licstationid['stationid'] = licstationid['stationid'].apply(lambda x: "'" + str(x) + "'")
licstationid='('+licstationid.stationid.str.cat(sep=',')+')'

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekdaytrip """
sql += """ FROM trip WHERE (stationid1 IN """+licstationid+""" OR stationid2 IN """+licstationid+""") AND startweekday NOT IN ('Saturday','Sunday') AND starthour =8 GROUP BY stationid1,stationid2 """
wkday = pd.read_sql(sql, conn)


sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekendtrip """
sql += """ FROM trip WHERE (stationid1 IN """+licstationid+""" OR stationid2 IN """+licstationid+""") AND startweekday IN ('Saturday','Sunday') AND starthour =8  GROUP BY stationid1,stationid2"""
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

df.to_csv(path + 'licod.csv', index=False, na_rep=0)
df=pd.read_csv(path+'licod.csv')

conn.close()

print(datetime.now()-start)



###STATION 8-9 USAGE COUNT 



# Calculate average weekday and weekend trips
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

sql = """SELECT *"""
sql += """ FROM station WHERE stationid IN """+licstationid+""" """
station = pd.read_sql(sql, conn)

sql = """SELECT startstationid AS stationid,COUNT(*) AS startweekdaytrip FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') AND starthour =8 """
sql += """GROUP BY startstationid"""
startwkday = pd.read_sql(sql, conn)
sql = """SELECT endstationid AS stationid,COUNT(*) AS endweekdaytrip FROM trip WHERE endweekday NOT IN ('Saturday','Sunday') AND endhour =8 """
sql += """GROUP BY endstationid"""
endwkday = pd.read_sql(sql, conn)


df = pd.merge(station, startwkday, how='left', on='stationid').sort_values('stationid')
df = pd.merge(df, endwkday, how='left', on='stationid').sort_values('stationid')
df['startweekdaytrip'] = np.where(pd.isna(df['startweekdaytrip']), 0, df['startweekdaytrip'].astype(float))
df['endweekdaytrip'] = np.where(pd.isna(df['endweekdaytrip']), 0, df['endweekdaytrip'].astype(float))
df['totalweekdaytrip'] = df['startweekdaytrip'] + df['endweekdaytrip']
df['avgweekdaytrip'] = np.where(df['weekdays'] == 0, 0, df['totalweekdaytrip'] / df['weekdays'])

sql = """SELECT startstationid AS stationid,COUNT(*) AS startweekendtrip FROM trip WHERE startweekday IN ('Saturday','Sunday') AND starthour =8 """
sql += """GROUP BY startstationid"""
startwkend = pd.read_sql(sql, conn)
sql = """SELECT endstationid AS stationid,COUNT(*) AS endweekendtrip FROM trip WHERE endweekday IN ('Saturday','Sunday') AND endhour =8 """
sql += """GROUP BY endstationid"""
endwkend = pd.read_sql(sql, conn)
df = pd.merge(df, startwkend, how='left', on='stationid').sort_values('stationid')
df = pd.merge(df, endwkend, how='left', on='stationid').sort_values('stationid')
df['startweekendtrip'] = np.where(pd.isna(df['startweekendtrip']), 0, df['startweekendtrip'].astype(float))
df['endweekendtrip'] = np.where(pd.isna(df['endweekendtrip']), 0, df['endweekendtrip'].astype(float))
df['totalweekendtrip'] = df['startweekendtrip'] + df['endweekendtrip']
df['avgweekendtrip'] = np.where(df['weekends'] == 0, 0, df['totalweekendtrip'] / df['weekends'])

df.to_csv(path + 'LICstationtrip8am.csv', index=False)

conn.close()









###STATION USAGE COUNT 



# Calculate average weekday and weekend trips
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

sql = """SELECT *"""
sql += """ FROM station WHERE stationid IN """+licstationid+""" """
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

df.to_csv(path + 'LICstationtrip.csv', index=False)





















# STATION USAGE COUNT 
