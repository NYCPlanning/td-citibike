#! /usr/bin/python3

import datetime
import sqlite3
import pandas as pd
import numpy as np
import requests
import multiprocessing as mp
import time

start=datetime.datetime.now()

pd.set_option('display.max_columns', 500)
path = '/home/mayijun/CITI2017/'
#path = 'C:/Users/Yijun Ma/Desktop/D/DOCUMENT/DCP2018/CITI2017/'
#path='J:/CITI2017/'


# od otp routes
conn = sqlite3.connect(path + 'CITI2017.sqlite3')

sql="""SELECT startstationid,endstationid FROM trip GROUP BY startstationid,endstationid"""
od=pd.read_sql(sql, conn)
od['odid']=['OD'+str(x)+str(y) for x,y in zip(od['startstationid'],od['endstationid'])]

sql = """SELECT * FROM station"""
station = pd.read_sql(sql, conn)

od=pd.merge(od,station,how='left',left_on='startstationid',right_on='stationid')
od=pd.merge(od,station,how='left',left_on='endstationid',right_on='stationid')
od=od[['odid','startstationid','stationname_x','stationlat_x','stationlong_x',
       'endstationid','stationname_y','stationlat_y','stationlong_y']]
od.columns=['odid','startstationid','startstationname','startstationlat','startstationlong',
            'endstationid','endstationname','endstationlat','endstationlong']
od['bgtime']=np.nan
od['bgroute']=''
od['fftime']=np.nan
od['ffroute']=''


def otproute(df):
    for i in df.index:
        if df.loc[i,'startstationid']==df.loc[i,'endstationid']:
            df.loc[i,'bgtime']=0
            df.loc[i,'bgroute']=''
            df.loc[i,'fftime']=0
            df.loc[i,'ffroute']=''
        else:
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'startstationlat'])+','+str(df.loc[i,'startstationlong'])
            url+='&toPlace='+str(df.loc[i,'endstationlat'])+','+str(df.loc[i,'endstationlong'])+'&mode=BICYCLE'
            url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(1/3)+'&triangleSlopeFactor='+str(1/3)+'&triangleTimeFactor='+str(1/3)
            url+='&bikeSpeed=3.5'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js.keys())[1]=='error':
                df.loc[i,'bgtime']=np.nan
                df.loc[i,'bgroute']=''
            else:
                df.loc[i,'bgtime']=js['plan']['itineraries'][0]['walkTime']
                df.loc[i,'bgroute']=js['plan']['itineraries'][0]['legs'][0]['legGeometry']['points']
            url='http://localhost:8801/otp/routers/default/plan?fromPlace='
            url+=str(df.loc[i,'startstationlat'])+','+str(df.loc[i,'startstationlong'])
            url+='&toPlace='+str(df.loc[i,'endstationlat'])+','+str(df.loc[i,'endstationlong'])+'&mode=BICYCLE'
            url+='&optimize=TRIANGLE&triangleSafetyFactor='+str(0)+'&triangleSlopeFactor='+str(1/2)+'&triangleTimeFactor='+str(1/2)
            url+='&bikeSpeed=3.5'
            headers={'Accept':'application/json'}
            req=requests.get(url=url,headers=headers)
            js=req.json()
            if list(js.keys())[1]=='error':
                df.loc[i,'fftime']=np.nan
                df.loc[i,'ffroute']=''
            else:
                df.loc[i,'fftime']=js['plan']['itineraries'][0]['walkTime']
                df.loc[i,'ffroute']=js['plan']['itineraries'][0]['legs'][0]['legGeometry']['points']
    time.sleep(0.05)
    return df


def parallelize(data, func):
    data_split = np.array_split(data,max(data.count()))
    pool = mp.Pool(mp.cpu_count()-2)
    data = pd.concat(pool.map(func, data_split))
    pool.close()
    pool.join()
    return data


if __name__=='__main__':
    od = parallelize(od, otproute)
    od.to_csv(path + 'odotproute.csv', index=False)
    od=pd.read_csv(path + 'odotproute.csv')
    od.to_sql('odotproute',conn,if_exists='replace',index=False)
    conn.close()
    print(datetime.datetime.now()-start)