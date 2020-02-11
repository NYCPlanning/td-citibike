

## validate the previous station data



setwd("C:/Users/F_Du/Desktop/Citi Bike/citibike_commuter/commuterdataset")

####process the dataload data


# 201306-201612
# 201701-201907   https://s3.amazonaws.com/tripdata/201803-citibike-tripdata.csv.zip

library(stringr)
library(RSQLite)
library(Rcpp)
library(dplyr)
library(DBI)


#2013

year=2013

for (month in 06:12){
  month
  month = str_pad(month, 2, pad=0, "left");
  
  downloadurl<-paste0("https://s3.amazonaws.com/tripdata/",year,month,"-citibike-tripdata.zip")
  destfile<-paste0(year,month,"-citibike-tripdata.zip")
  
  
  
  download.file(downloadurl,destfile=destfile)
  locatonfile<-paste0("C:/Users/F_Du/Desktop/Citi Bike/citibike_commuter/commuterdataset/",destfile)
  locatonfile
  unzip(locatonfile)
  csvfile<-unzip(locatonfile)
  csvfile
  Citibikemonth<-read.csv(csvfile[1])
  
  
  names(Citibikemonth) <- tolower(names(Citibikemonth))
  
  Citibikemonthstartstation<-Citibikemonth%>%
    group_by(start.station.id,start.station.name,start.station.latitude,start.station.longitude)%>%
    rename(stationid = start.station.id,stationname=start.station.name,lat=start.station.latitude,long=start.station.longitude)%>%
    summarise(n=n())
  
  Citibikemonthendstation<-Citibikemonth%>%
    group_by(end.station.id,end.station.name,end.station.latitude,end.station.longitude)%>%
    rename(stationid = end.station.id,stationname=end.station.name,lat=end.station.latitude,long=end.station.longitude)%>%
    summarise(n=n())
  
  Citibikemonthstation<-unique(merge(Citibikemonthstartstation, Citibikemonthendstation, by = c("stationid","stationname","lat","long"), all=TRUE))
  Citibikemonthstation[is.na(Citibikemonthstation)] <- 0
  
  if(month =="06" ){
    print(year)
    Citibikemonthstationyear<-Citibikemonthstation
  }else{
    Citibikemonthstationyear<-unique(rbind(Citibikemonthstationyear,Citibikemonthstation))
  }
  
  
  
  
}

Citibikemonthstationyear<-Citibikemonthstationyear%>%
  group_by(stationid,stationname,lat,long)%>%
  summarise(frequency=(sum(n.x)+sum(n.y)))
library(dplyr)
Citibikemonthstationyear <- arrange(Citibikemonthstationyear, stationid, -frequency)
Citibikemonthstationyear <- Citibikemonthstationyear[!duplicated(Citibikemonthstationyear$stationid),]



Citibikemonthstationyear$year<-year
filename<-paste0(year,"station.csv")
write.csv(Citibikemonthstationyear,filename)
# 
# 


#2014-2016
for (year in 2014: 2016) {
  
  for (month in 01:12){
    
    month = str_pad(month, 2, pad=0, "left");
    
    downloadurl<-paste0("https://s3.amazonaws.com/tripdata/",year,month,"-citibike-tripdata.zip")
    destfile<-paste0(year,month,"-citibike-tripdata.zip")
    
    download.file(downloadurl,destfile=destfile)
    locatonfile<-paste0("C:/Users/F_Du/Desktop/Citi Bike/citibike_commuter/commuterdataset/",destfile)
    Citibikemonth<-read.csv(unzip(locatonfile))
    
    names(Citibikemonth) <- tolower(names(Citibikemonth))
    
    Citibikemonthstartstation<-Citibikemonth%>%
      group_by(start.station.id,start.station.name,start.station.latitude,start.station.longitude)%>%
      rename(stationid = start.station.id,stationname=start.station.name,lat=start.station.latitude,long=start.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthendstation<-Citibikemonth%>%
      group_by(end.station.id,end.station.name,end.station.latitude,end.station.longitude)%>%
      rename(stationid = end.station.id,stationname=end.station.name,lat=end.station.latitude,long=end.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthstation<-unique(merge(Citibikemonthstartstation, Citibikemonthendstation, by = c("stationid","stationname","lat","long"), all=TRUE))
    Citibikemonthstation[is.na(Citibikemonthstation)] <- 0
    
    if(month =="01" ){
      print(year)
      Citibikemonthstationyear<-Citibikemonthstation
    }else{
      Citibikemonthstationyear<-unique(rbind(Citibikemonthstationyear,Citibikemonthstation))
    }
    
    
    
    
  }
  Citibikemonthstationyear<-Citibikemonthstationyear%>%
    group_by(stationid,stationname,lat,long)%>%
    summarise(frequency=(sum(n.x)+sum(n.y)))
  library(dplyr)
  Citibikemonthstationyear <- arrange(Citibikemonthstationyear, stationid, -frequency)
  Citibikemonthstationyear <- Citibikemonthstationyear[!duplicated(Citibikemonthstationyear$stationid),]
  
  
  
  Citibikemonthstationyear$year<-year
  filename<-paste0(year,"station.csv")
  write.csv(Citibikemonthstationyear,filename)
  # 
  # 
} 





#2017-2018
for (year in 2017: 2018) {
  
  for (month in 01:12){
    
    month = str_pad(month, 2, pad=0, "left");
    
    downloadurl<-paste0("https://s3.amazonaws.com/tripdata/",year,month,"-citibike-tripdata.csv.zip")
    destfile<-paste0(year,month,"-citibike-tripdata.zip")
    
    download.file(downloadurl,destfile=destfile)
    locatonfile<-paste0("C:/Users/F_Du/Desktop/Citi Bike/citibike_commuter/commuterdataset/",destfile)
    Citibikemonth<-read.csv(unzip(locatonfile))
    
    names(Citibikemonth) <- tolower(names(Citibikemonth))
    
    Citibikemonthstartstation<-Citibikemonth%>%
      group_by(start.station.id,start.station.name,start.station.latitude,start.station.longitude)%>%
      rename(stationid = start.station.id,stationname=start.station.name,lat=start.station.latitude,long=start.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthendstation<-Citibikemonth%>%
      group_by(end.station.id,end.station.name,end.station.latitude,end.station.longitude)%>%
      rename(stationid = end.station.id,stationname=end.station.name,lat=end.station.latitude,long=end.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthstation<-unique(merge(Citibikemonthstartstation, Citibikemonthendstation, by = c("stationid","stationname","lat","long"), all=TRUE))
    Citibikemonthstation[is.na(Citibikemonthstation)] <- 0
    
    if(month =="01" ){
      print(year)
      Citibikemonthstationyear<-Citibikemonthstation
    }else{
      Citibikemonthstationyear<-unique(rbind(Citibikemonthstationyear,Citibikemonthstation))
    }
    
    
    
    
  }
  
  Citibikemonthstationyear<-Citibikemonthstationyear%>%
    group_by(stationid,stationname,lat,long)%>%
    summarise(frequency=(sum(n.x)+sum(n.y)))
  library(dplyr)
  Citibikemonthstationyear <- arrange(Citibikemonthstationyear, stationid, -frequency)
  Citibikemonthstationyear <- Citibikemonthstationyear[!duplicated(Citibikemonthstationyear$stationid),]
  
  
  
  Citibikemonthstationyear$year<-year
  filename<-paste0(year,"station.csv")
  write.csv(Citibikemonthstationyear,filename)
  # 
  # 
} 







#2019
for (year in 2019: 2019) {

  for (month in 01:12){
    
    month = str_pad(month, 2, pad=0, "left");
    
    downloadurl<-paste0("https://s3.amazonaws.com/tripdata/",year,month,"-citibike-tripdata.csv.zip")
    destfile<-paste0(year,month,"-citibike-tripdata.zip")
    
    
    download.file(downloadurl,destfile=destfile)
    locatonfile<-paste0("C:/Users/F_Du/Desktop/Citi Bike/citibike_commuter/commuterdataset/",destfile)
    locatonfile
    unzip(locatonfile)
    csvfile<-unzip(locatonfile)
    csvfile
    Citibikemonth<-read.csv(csvfile[1])
    
    
    names(Citibikemonth) <- tolower(names(Citibikemonth))
    
    Citibikemonthstartstation<-Citibikemonth%>%
      group_by(start.station.id,start.station.name,start.station.latitude,start.station.longitude)%>%
      rename(stationid = start.station.id,stationname=start.station.name,lat=start.station.latitude,long=start.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthendstation<-Citibikemonth%>%
      group_by(end.station.id,end.station.name,end.station.latitude,end.station.longitude)%>%
      rename(stationid = end.station.id,stationname=end.station.name,lat=end.station.latitude,long=end.station.longitude)%>%
      summarise(n=n())
    
    Citibikemonthstation<-unique(merge(Citibikemonthstartstation, Citibikemonthendstation, by = c("stationid","stationname","lat","long"), all=TRUE))
    Citibikemonthstation[is.na(Citibikemonthstation)] <- 0
    
    if(month =="01" ){
      print(year)
      Citibikemonthstationyear<-Citibikemonthstation
    }else{
      Citibikemonthstationyear<-unique(rbind(Citibikemonthstationyear,Citibikemonthstation))
    }
    
    
    
    
  }
  
  Citibikemonthstationyear<-Citibikemonthstationyear%>%
    group_by(stationid,stationname,lat,long)%>%
    summarise(frequency=(sum(n.x)+sum(n.y)))
  library(dplyr)
  Citibikemonthstationyear <- arrange(Citibikemonthstationyear, stationid, -frequency)
  Citibikemonthstationyear <- Citibikemonthstationyear[!duplicated(Citibikemonthstationyear$stationid),]
  
  
  
  Citibikemonthstationyear$year<-year
  filename<-paste0(year,"station.csv")
  write.csv(Citibikemonthstationyear,filename)
  # 
  # 
} 



#combine all stations
stations2013_2019=data.frame()
for (year in 2013:2019){
  stations2013_2019<-rbind(stations2013_2019,read.csv(paste0(year,'station.csv'),stringsAsFactors = FALSE,colClasses = 'character'))
}
write.csv(stations2013_2019,"stations2013_2019.csv")


stations2013_2019<-stations2013_2019%>%
  filter(stations2013_2019$frequency>100 & !is.na(stations2013_2019$lat))
write.csv(stations2013_2019,"stations2013_2019.csv")
