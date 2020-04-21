#The purpose of this script is to run through NC files from CaSPAr and convert them into a timeseries of daily precipitation, TMax, and TMin.
#CaSPAr organizes files as netcdfs, where one file represents one day for one member. For example, a file may correspond to April 23rd, 2018, member 6. This one file would contain data for all lats/lons specified in the data request. What this script will do is restack the data into CSVs. Each CSV will contain data for only one lat/lon pair and one member, but for all days of the year. For example, a file may contain daily data for the location 44,280, for member 0.



#This will take 2 directories - a precipitation directory and a temperature directory, as well as a sequence of ensemble members to run.

library(ncdf4)
library(parallel)
#num_cores <- detectCores()
#cl <- makeCluster(num_cores - 8)

#Currently not parallelized - the recursive file reading may cause problems. I may later try to parallelize multiple ensemble memebers.

DailyMax = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
  
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = max(InputArray[i,j,])
    }
  }
  
  return(OutArray)
}

DailyMin = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
  
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = min(InputArray[i,j,])
    }
  }
  
  return(OutArray)
}

DailyMean = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
  
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = mean(InputArray[i,j,])
    }
  }
  
  return(OutArray)
}

DailyDifference = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
  
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = InputArray[i,j,8] - InputArray[i,j,1]
    }
  }
  
  return(OutArray)
}

######
GetLastWorkingFile = function(Variable, FileDate, MemberEnding, Directory, Recurse){    #Full string name of variable, date, file name ending, directory, and number of recursions
  
#The function that does the heavy lifting when it comes to getting data from NC file. It's fed a file location, ending, and date (the date is used to construct the file name). It attempts to open and read the file, but if that isn't possible, the function recurses to the previous day and will attempt to search that file instead. As an example, if you had one week of forecasts with 8 days of data in them each, it would go day to day, grabbing the first 24 hours from each. If the third day was missing, corrupted, or otherwise unusuable, it would recurse back the second day and use hours 30-54 to infill. Furthermore, at the end of the week, the function, if desired, could recurse back to the last file to create a second week of data by taking hours 30-198.
  
  if(Recurse >= 32) return("END")     #Past 32 days out, there is clearly no data left.
  
  Flag = FALSE
  
  DateString = format(FileDate, format = '%Y%m%d')
  FilePath  = paste(Directory, DateString, MemberEnding, sep='')
  
  if(!file.exists(FilePath)) return(GetLastWorkingFile(Variable, FileDate - 1, MemberEnding, Directory, Recurse + 1))
    
  Data = nc_open(FilePath)
  
  for(VarCount in 1:Data$nvar){
    if(Data$var[[VarCount]]$name == Variable){
      Flag = TRUE
      break
    }
  }
  
  if(FileDate == "2019-08-10") Flag = FALSE
  if(FileDate == "2019-09-12") Flag = FALSE
  
  if(!Flag){
    
    return(GetLastWorkingFile(Variable, FileDate - 1, MemberEnding, Directory, Recurse + 1))
    nc_close(Data)
    
  } 
  
  DataArray = ncvar_get(Data, Variable)
  LatArr = ncvar_get(Data, "lat")
  LonArr = ncvar_get(Data, "lon")
  
  nc_close(Data)
  
  #DataArray is of dimensions [Lat * Lon * TIMESTEPS] where there are 8 timesteps per day. Under normal circumstances, we want to use steps 3-10. The reasoning behind this is fairly simple. Steps 1, 2, and 3 correspond to hours 0, 3, and 6. Given that Canada's midnight is roughly, on average, UTC 6, we can think of overall, our day being offset by 6 hours.
  StartTimeIndex = (Recurse * 8) + 3
  DataArray = DataArray[,, StartTimeIndex:(StartTimeIndex + 7)]
  
  if(Variable == TempVar){
    OutArray = array(0, c(90, 360, 3))
    
    TMax = DailyMax(DataArray)
    TMin = DailyMin(DataArray)
    
    for(LonCount in 1:dim(LatArr)[1]){				#Loop through the lat,lon - dimension of both lat and lon arrays are identical
      for(LatCount in 1:dim(LatArr)[2]){
        if(is.nan(TMax[LonCount, LatCount])){ #Assume that if TMax is invalid at a certain location, Tmin must also be invalid, and vice versa
          next
        }	
        Lat = floor(LatArr[LonCount, LatCount] + 0.5)
        if(LonArr[LonCount, LatCount] >= 0.5){
          Lon = floor(LonArr[LonCount, LatCount] + 0.5)
        } else {
          Lon = 360 + floor(LonArr[LonCount, LatCount] + 0.5)			#It runs 0 -> 180, jumps to -180, then -> 0
        }
        TMaxAdd = TMax[LonCount, LatCount]		
        TMinAdd = TMin[LonCount, LatCount]		
        
        OutArray[Lat, Lon, 1] = OutArray[Lat, Lon, 1] + TMaxAdd
        OutArray[Lat, Lon, 2] = OutArray[Lat, Lon, 2] + TMinAdd
        OutArray[Lat, Lon, 3] = OutArray[Lat, Lon, 3] + 1
        
      }
      
    }
    
    for(Lon in 1:360){									#Now we need to do it again, but this time we apply the averaging to our summed values
      for(Lat in 1:90){
        if(OutArray[Lat, Lon, 2] == 0){
          next
        }
        OutArray[Lat, Lon, 1] = OutArray[Lat, Lon, 1] / OutArray[Lat, Lon, 3]
        OutArray[Lat, Lon, 2] = OutArray[Lat, Lon, 2] / OutArray[Lat, Lon, 3]
      }
      
    }
    
    
  } else if(Variable == PrecipVar){
    OutArray = array(0, c(90, 360, 2))
    
    PMean = DailyDifference(DataArray)
    
    for(LonCount in 1:dim(LatArr)[1]){				#Loop through the lat,lon - dimension of both lat and lon arrays are identical
      for(LatCount in 1:dim(LatArr)[2]){
        if(is.nan(PMean[LonCount, LatCount])){ #Assume that if TMax is invalid at a certain location, Tmin must also be invalid, and vice versa
          next
        }	
        Lat = floor(LatArr[LonCount, LatCount] + 0.5)
        if(LonArr[LonCount, LatCount] >= 0){
          Lon = floor(LonArr[LonCount, LatCount] + 0.5)
        } else {
          Lon = 360 + floor(LonArr[LonCount, LatCount] + 0.5)			#It runs 0 -> 180, jumps to -180, then -> 0
        }
        PMeanAdd = PMean[LonCount, LatCount]		#24 hour precip! We want daily!
        
        OutArray[Lat, Lon, 1] = OutArray[Lat, Lon, 1] + PMeanAdd
        OutArray[Lat, Lon, 2] = OutArray[Lat, Lon, 2] + 1
        
      }
    }
    
    for(Lon in 1:360){									#Now we need to do it again, but this time we apply the averaging to our summed values
      for(Lat in 1:90){
        if(OutArray[Lat, Lon, 2] == 0){
          next
        }
        OutArray[Lat, Lon, 1] = OutArray[Lat, Lon, 1] / OutArray[Lat, Lon, 2]
        OutArray[Lat, Lon, 1] = OutArray[Lat, Lon, 1] * 1000
      }
      
    }
    
  }
  
  print(paste('Recursed ', Recurse, ' times', sep=''))
  return(OutArray)
  
}

#########################################################################################
#   Main   #


UsePrecip = TRUE                               #Whether or not to include precipitaiton, and if so, which directory to pull from
PrecipDir = 'E:\\CaSPArMain\\Summer2019\\'
PrecipVar = 'GEPS_P_PR_SFC'

UseTemp = FALSE                                  #Whether or not to include temperature, and if so, which directory to pull from
TempDir = 'E:\\CaSPArMain\\Summer2019\\'
#TempVar = 'GEPS_P_TT_1.5m'
TempVar = 'GEPS_P_TT_1.5m'

Ensembles = (0:0)                               #Members of interest, as 0:20 or however

DateRange = as.Date(c("2019-07-01", "2019-10-22"))
Dates = DateRange[2] - DateRange[1] + 1

OutputArray = array(-999, c(90,360,Dates,3))    #Lat, Lon, Time, (Tmax Tmin Precip)

for(Member in Ensembles){                       
  
  if(Member < 10){
    EndStr = paste("00_00", Member, ".nc", sep="")		#Generate the current file name
  } else {
    EndStr = paste("00_0", Member, ".nc", sep="")
  }
  
  for(Date in 1:Dates){                           #Go through each day in the time period specified and try to get data for that day
    DateCount = Date
    Date = DateRange[1] + Date - 1
    DateString = format(Date, format = '%Y%m%d')
    
    FileString = paste(DateString, EndStr, sep='')
    
    if(UsePrecip){
      PrecipFile = paste(PrecipDir, FileString, sep='')
      TodayPrecip = GetLastWorkingFile(PrecipVar, Date, EndStr, PrecipDir, 0)     #The function will either return the day's data or a stop condition
      if(TodayPrecip[1] == 'END') break
      OutputArray[,,DateCount,1] = TodayPrecip[,,1]
    }
    
    if(UseTemp){
      TempFile = paste(TempDir, FileString, sep='')
      TodayTemp = GetLastWorkingFile(TempVar, Date, EndStr, TempDir, 0)
      if(TodayTemp[1] == 'END') break
      OutputArray[,,DateCount,2:3] = TodayTemp[,,1:2]
    }
    
  }
  
  for(Lat in 15:90){
    for(Lon in 180:360){
      OutName = paste('E:\\ForecastSummer19\\', Member, '\\', Lat, '_', Lon, '.csv',sep='')
      
      write.table(OutputArray[Lat,Lon,,1], OutName, quote=FALSE, sep=',')
      
    }
  }
  
  
}

