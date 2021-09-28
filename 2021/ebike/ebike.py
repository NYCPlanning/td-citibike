import pandas as pd
import geopandas as gpd
import requests
import shapely
import datetime
import time
import pytz
import os


pd.set_option('display.max_columns', None)
# path='C:/Users/mayij/Desktop/DOC/DCP2021/CITIBIKE/EBIKE/'
path='/home/mayijun/EBIKE/'


# Download
endtime=datetime.datetime(2021,10,1,23,0,0,0,pytz.timezone('US/Eastern'))
while datetime.datetime.now(pytz.timezone('US/Eastern'))<endtime:
    timestamp=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')
    url='https://gbfs.citibikenyc.com/gbfs/en/station_status.json'
    eb=pd.read_json(url)
    eb=pd.DataFrame(eb['data'][0])
    eb=eb[['station_id','num_bikes_available']].reset_index(drop=True)
    eb.to_csv(path+'DATA/'+timestamp.replace('-','').replace(':','').replace(' ','_')+'.csv',index=False)
    print(timestamp)
    time.sleep(60)


# # Summary
# url='https://gbfs.citibikenyc.com/gbfs/en/station_information.json'
# df=pd.read_json(url)
# df=pd.DataFrame(df['data'][0])
# df=df[['station_id','name','lat','lon']].reset_index(drop=True)
# for i in sorted(os.listdir(path+'DATA/')):
#     tp=pd.read_csv(path+'DATA/'+i,dtype=float,converters={'station_id':str})
#     tp.columns=['station_id',i.replace('.csv','')]
#     df=pd.merge(df,tp,how='left',on='station_id')
# df=df.melt(id_vars=['station_id','name','lat','lon'],var_name='time',value_name='ebike')
# df=df[df['ebike']>0].reset_index(drop=True)
# df=df.groupby(['station_id','name','lat','lon'],as_index=False).agg({'ebike':'count'}).reset_index(drop=True)
# df=gpd.GeoDataFrame(df,geometry=[shapely.geometry.Point(x,y) for x,y in zip(df['lon'],df['lat'])],crs=4326)
# df.to_file(path+'EBIKE.geojson',driver='GeoJSON')












