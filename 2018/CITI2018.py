import sqlite3
import pandas as pd
import numpy as np
# =============================================================================
# import requests
# import polyline
# =============================================================================

pd.set_option('display.max_columns', 500)
path = 'C:/Users/F_Du/Desktop/Citi Bike/CitiBikeGH-master/CitiBikeGH-master/citibikedata/2018/'


# Import raw data into SQLite database
conn = sqlite3.connect(path + 'CITI2018.sqlite3')


###??? perference in opentripplanner

for i in range(1, 13):     
    tp = pd.read_csv(path + '2018' + str(i).zfill(2) + '-citibike-tripdata.csv', dtype=object)
    tp.columns = ['tripduration', 'starttime', 'endtime', 'startstationid', 'startstationname', 'startstationlat','startstationlong', 'endstationid', 'endstationname', 'endstationlat', 'endstationlong', 'bikeid','usertype', 'birthyear', 'gender'] 
    tp['startstationid']=['ST'+str(x).zfill(4) for x in tp['startstationid']]
    tp['endstationid']=['ST'+str(x).zfill(4) for x in tp['endstationid']]
    jc = pd.read_csv(path + 'JC-2018' + str(i).zfill(2) + '-citibike-tripdata.csv', dtype=object)
    jc.columns = ['tripduration', 'starttime', 'endtime', 'startstationid', 'startstationname', 'startstationlat',
                  'startstationlong', 'endstationid', 'endstationname', 'endstationlat', 'endstationlong', 'bikeid',
                   'usertype', 'birthyear', 'gender']
    jc['startstationid']=['ST'+x.zfill(4) for x in jc['startstationid']]
    jc['endstationid']=['ST'+x.zfill(4) for x in jc['endstationid']]   

    df = tp
    df = df.sort_values(by=['starttime', 'startstationid'], axis=0, ascending=True)
    df['tripid'] = ['TP2018' + str(i).zfill(2) + y for y in [str(x).zfill(7) for x in range(1, max(df.count()) + 1)]]
    
#??? what does it means.dt.date
    
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
    df['age'] = np.where(pd.isna(df['birthyear']), np.nan, 2018 - df['birthyear'])
    df['gender'] = np.where(df['gender'] == '1', 'Male', np.where(df['gender'] == '2', 'Female', 'Unknown'))
    df = df[['tripid', 'starttime', 'startdate', 'startmonth', 'startweekday', 'starthour', 'startstationid', 'startstationname',
             'startstationlat', 'startstationlong', 'endtime', 'enddate', 'endmonth', 'endweekday', 'endhour', 'endstationid',
             'endstationname', 'endstationlat', 'endstationlong', 'odid', 'tripduration', 'bikeid', 'usertype', 'birthyear',
             'age', 'gender']]
    df.to_sql('trip', conn, if_exists='append', index=False)

conn.close()





# Create station list
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

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
df.to_csv(path + 'station.csv', index=False)  # Manually remove duplicates

conn.close()







# Calculate station days
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

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
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

station = pd.read_csv(path + 'station.csv')
wkd = pd.read_csv(path + 'stationdays.csv')
station=station[['stationid','stationname','stationlat','stationlong']]
df = pd.merge(station, wkd, how='outer', on='stationid').sort_values('stationid')
df.to_sql('station', conn, if_exists='replace', index=False)

conn.close()
















# Calculate average weekday and weekend trips
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

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












# Calculate hourly distribution
conn = sqlite3.connect(path + 'CITI2018.sqlite3')

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





# =============================================================================
# 
# 
# 
# 
# 
# # od otp routes
# conn = sqlite3.connect(path + 'CITI2017.sqlite3')
# 
# sql="""SELECT startstationid,endstationid FROM trip GROUP BY startstationid,endstationid"""
# od=pd.read_sql(sql, conn)
# od['odid']=['OD'+x+y for x,y in zip(od['startstationid'],od['endstationid'])]
# 
# sql = """SELECT * FROM station"""
# station = pd.read_sql(sql, conn)
# 
# 
# od=pd.merge(od,station,how='left',left_on='startstationid',right_on='stationid')
# od=pd.merge(od,station,how='left',left_on='endstationid',right_on='stationid')
# od=od[['odid','startstationid','stationname_x','stationlat_x','stationlong_x',
#        'endstationid','stationname_y','stationlat_y','stationlong_y']]
# od.columns=['odid','startstationid','startstationname','startstationlat','startstationlong',
#             'endstationid','endstationname','endstationlat','endstationlong']
# od['bgtime']=np.nan
# od['bgroute']=''
# od['fftime']=np.nan
# od['ffroute']=''
# 
# 
# 
# for i in range(0,max(od.count())):
#     if od.loc[i,'startstationid']==od.loc[i,'endstationid']:
#         od.loc[i,'bgtime']=0
#         od.loc[i,'bgroute']=''
#         od.loc[i,'fftime']=0
#         od.loc[i,'ffroute']=''
#     else:
#         url='http://localhost:8801/otp/routers/default/plan?fromPlace='
#         url+=str(od.loc[i,'startstationlat'])+','+str(od.loc[i,'startstationlong'])
#         url+='&toPlace='+str(od.loc[i,'endstationlat'])+','+str(od.loc[i,'endstationlong'])+'&mode=BICYCLE'
#         url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(1/3)+'&triangleSlopeFactor='+str(1/3)+'&triangleTimeFactor='+str(1/3)
#         url+='&bikeSpeed=3.5'
#         headers={'Accept':'application/json'}
#         req=requests.get(url=url,headers=headers)
#         js=req.json()
#         if list(js.keys())[1]=='error':
#             od.loc[i,'bgtime']=np.nan
#             od.loc[i,'bgroute']=''
#         else:
#             od.loc[i,'bgtime']=js['plan']['itineraries'][0]['walkTime']
#             od.loc[i,'bgroute']=js['plan']['itineraries'][0]['legs'][0]['legGeometry']['points']
#         url='http://localhost:8801/otp/routers/default/plan?fromPlace='
#         url+=str(od.loc[i,'startstationlat'])+','+str(od.loc[i,'startstationlong'])
#         url+='&toPlace='+str(od.loc[i,'endstationlat'])+','+str(od.loc[i,'endstationlong'])+'&mode=BICYCLE'
#         url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(0)+'&triangleSlopeFactor='+str(1/2)+'&triangleTimeFactor='+str(1/2)
#         url+='&bikeSpeed=3.5'
#         headers={'Accept':'application/json'}
#         req=requests.get(url=url,headers=headers)
#         js=req.json()
#         if list(js.keys())[1]=='error':
#             od.loc[i,'fftime']=np.nan
#             od.loc[i,'ffroute']=''
#         else:
#             od.loc[i,'fftime']=js['plan']['itineraries'][0]['walkTime']
#             od.loc[i,'ffroute']=js['plan']['itineraries'][0]['legs'][0]['legGeometry']['points']
# 
# od.to_csv(path + 'odotproute2.csv', index=False)
# od=pd.read_csv(path + 'odotproute2.csv')
# od.to_sql('odotproute2',conn,if_exists='replace',index=False)
# 
# conn.close()
# 
# 
# 
# 
# ###???
# ###what is the following script
# 
# #
# conn = sqlite3.connect(path + 'CITI2017.sqlite3')
# cur=conn.cursor()
# sql="""ALTER TABLE trip"""
# sql+=""" ADD COLUMN bgtime REAL"""
# cur.execute(sql)
# sql="""ALTER TABLE trip"""
# sql+=""" ADD COLUMN fftime REAL"""
# cur.execute(sql)
# sql="""UPDATE trip"""
# sql+=""" SET bgtime=(SELECT bgtime FROM odotproute WHERE trip.odid=odotproute.odid)"""
# cur.execute(sql)
# conn.commit()
# conn.close()
# 
# 
# 
# 
# sql="""SELECT * FROM trip LIMIT 100"""
# tp=pd.read_sql(sql,conn)
# tp.to_sql('tp',conn,if_exists='replace',index=False)
# sql="""UPDATE tp"""
# sql+=""" SET fftime=(SELECT fftime FROM odotproute WHERE tp.odid=odotproute.odid)"""
# cur.execute(sql)
# conn.commit()
# 
# 
# conn.close()
# 
# 
# 
# 
# 
# polyline.decode(pt)
# 
# 
# # Check
# conn = sqlite3.connect(path + 'CITI2017.sqlite3')
# sql="""SELECT * from odotproute"""
# od=pd.read_sql(sql, conn)
# sql="""SELECT odid,count(*) AS trip FROM trip GROUP BY odid ORDER BY trip DESC"""
# tp=pd.read_sql(sql, conn)
# od=pd.merge(od,tp,how='left',on='odid')
# od=od.loc[pd.isna(od['bgroute'])&(od['startstationid']!=od['endstationid']),:].sort_values('trip',ascending=False)
# conn.close()
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# 
# =============================================================================







# Downtown Brooklyn
# Downtown Brooklyn hourly distribution
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

dbk = "(3440,323,390,239,3232,324,243,310,3486,395,3455,298)"

sql = """SELECT startstationid AS stationid,starthour AS hour,count(*) as weekdaystarttrip FROM trip WHERE startweekday NOT IN ('Saturday','Sunday') """
sql += """ AND startstationid IN """ + dbk
sql += """ GROUP BY startstationid,starthour"""
start = pd.read_sql(sql, conn)
start = start.pivot(index='stationid', columns='hour', values='weekdaystarttrip')
start['total'] = start.sum(axis=1)
start.loc['weekdaystart', :] = start.sum(axis=0)
start.loc['weekdaystart', range(0, 24)] = start.loc['weekdaystart', range(0, 24)] / start.loc['weekdaystart', 'total']
start = start.loc['weekdaystart', :]
start = start.reset_index()

sql = """SELECT endstationid AS stationid,endhour AS hour,count(*) as weekdayendtrip FROM trip WHERE endweekday NOT IN ('Saturday','Sunday') """
sql += """ AND endstationid IN """ + dbk
sql += """ GROUP BY endstationid,endhour"""
end = pd.read_sql(sql, conn)
end = end.pivot(index='stationid', columns='hour', values='weekdayendtrip')
end['total'] = end.sum(axis=1)
end.loc['weekdayend', :] = end.sum(axis=0)
end.loc['weekdayend', range(0, 24)] = end.loc['weekdayend', range(0, 24)] / end.loc['weekdayend', 'total']
end = end.loc['weekdayend', :]
end = end.reset_index()

df = pd.merge(start, end, how='outer', on='hour')

sql = """SELECT startstationid AS stationid,starthour AS hour,count(*) as weekendstarttrip FROM trip WHERE startweekday IN ('Saturday','Sunday') """
sql += """ AND startstationid IN """ + dbk
sql += """ GROUP BY startstationid,starthour"""
start = pd.read_sql(sql, conn)
start = start.pivot(index='stationid', columns='hour', values='weekendstarttrip')
start['total'] = start.sum(axis=1)
start.loc['weekendstart', :] = start.sum(axis=0)
start.loc['weekendstart', range(0, 24)] = start.loc['weekendstart', range(0, 24)] / start.loc['weekendstart', 'total']
start = start.loc['weekendstart', :]
start = start.reset_index()

df = pd.merge(df, start, how='outer', on='hour')

sql = """SELECT endstationid AS stationid,endhour AS hour,count(*) as weekendendtrip FROM trip WHERE endweekday IN ('Saturday','Sunday') """
sql += """ AND endstationid IN """ + dbk
sql += """ GROUP BY endstationid,endhour"""
end = pd.read_sql(sql, conn)
end = end.pivot(index='stationid', columns='hour', values='weekendendtrip')
end['total'] = end.sum(axis=1)
end.loc['weekendend', :] = end.sum(axis=0)
end.loc['weekendend', range(0, 24)] = end.loc['weekendend', range(0, 24)] / end.loc['weekendend', 'total']
end = end.loc['weekendend', :]
end = end.reset_index()

df = pd.merge(df, end, how='outer', on='hour')

df.to_csv(path + 'dbkhourly.csv', index=False, na_rep=0)

conn.close()


# Downtown Brooklyn origin-destination
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

dbk = "(3440,323,390,239,3232,324,243,310,3486,395,3455,298)"

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekdaytrip """
sql += """FROM trip WHERE (stationid1 IN """+dbk+""" OR stationid2 IN """+dbk+""") AND startweekday NOT IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
wkday = pd.read_sql(sql, conn)

sql = """SELECT MIN(startstationid,endstationid) AS stationid1,MAX(startstationid,endstationid) AS stationid2,count(*) as totalweekendtrip """
sql += """FROM trip WHERE (stationid1 IN """+dbk+""" OR stationid2 IN """+dbk+""") AND startweekday IN ('Saturday','Sunday') GROUP BY stationid1,stationid2"""
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

df.to_csv(path + 'dbkod.csv', index=False, na_rep=0)

conn.close()