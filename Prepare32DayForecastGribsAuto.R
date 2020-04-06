library(ncdf4)

#This script will read the forecast grib files and convert the data to location-specific CSV format. Each CSV will contain all daily data from the start of the forecast period to the end. This is usually the first thursday of a month to its end. Each CSV will contain data for all ensemble members.



#Custom functions. Called like Variable = Dailymax(ValuesArray). Takes an array of n values, and calculates a max, min, mean, or difference between the first and last. Depends on which function you call.

#Listen, I hadn't realized how useful apply was. Cut me some slack.

DailyMax = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
                                    #Largest of Z
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = max(na.omit(InputArray[i,j,]))
    }
  }
  
  return(OutArray)
}

DailyMin = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
                                    #Smallest of Z
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = min(na.omit(InputArray[i,j,]))
    }
  }
  
  return(OutArray)
}

DailyMean = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
                                     #Mean of Z
  x = dim(InputArray)[1]
  y = dim(InputArray)[2]
  
  OutArray = array(0, c(x, y))
  
  for(i in 1:x){
    for(j in 1:y){
      OutArray[i,j] = mean(na.omit(InputArray[i,j,]))
    }
  }
  
  return(OutArray)
}

DailyDifference = function(InputArray){    #Assumes a 3 dimensional array x y z - generates maximums for each x y (in effect 'flattens' a z height)
                                           #Zn - Z1
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

#ForecastDate refers to the first day of the long term forecast, usually the first thursday of the month. Lastday refers to the last day of the month that the forecast ends in. MainDir is simply the main AutomaticDO directory.

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
LastDay = as.Date(commandArgs(trailingOnly = TRUE)[3])
MainDir = paste0(commandArgs(trailingOnly = TRUE)[4], '\\')


FormatDir = paste0(MainDir, '\\24HForecasts\\CSV\\')   #Since the files in here represent the location we want, we can list out all the files to find every lat,lon pair that we want to look at.
FormatFiles = list.files(FormatDir)
FormatLength = length(FormatFiles)

BetweenStr = '_latlon0p5x0p5_'    #It goes between the variable and the date components of the file name. Kinda dumb tbh.
MainSearchDir = paste0(MainDir, '24HForecasts\\Grib/dd.weather.gc.ca/ensemble/geps/grib2/raw/00/')    #The folder where the forecasts are kept
InvokeWgrib = paste0(MainDir, 'wgrib2/wgrib2')        #where my wgrib program is kept
WgribArgs = '-set_ext_name 1 -netcdf'   #arguments to make the wgrib output nice
TempFile = paste0(MainDir, 'Temp/Temp.nc')  #A temporary nc file. We convert a grib to this file, read it, then delete it. Saves some disk space.

PrecipArray = array(0, c(360, 181, 21))   #Lon, Lat, Members
TemperatureArray = array(0, c(360, 181, 21))   #Lon, Lat, Members

LocalDir = paste0(MainDir, 'MonthlyForecasts/', ForecastDate, '/')    #Since we copy the gribs to a local directory, this is where we put them previously
FormatDate = format(ForecastDate, '%Y%m%d')                           #The forecast date, formatted to remove dashes
OutDir = paste0(MainDir, 'MonthlyForecasts/', ForecastDate, 'CSV/')   #Our export location for the end result of this script

Variables = c('APCP_SFC_0', 'TMP_TGL_2m')     #Precipitation and Temperature variables

DaysInForecast = LastDay - ForecastDate + 1  #Difference of days, but include the start day

ExportArray = array(-99, c(360,181,21, DaysInForecast, 3))    #Lon, Lat, Members, Days, (Precipitation, Tmax, Tmin)

#############

if(!dir.exists(OutDir)) dir.create(OutDir)

for(Variable in Variables){   #Handle each variable individually to make thing easy. First Precip, then Temp.
  
  Files = list.files(MainSearchDir, pattern = paste(Variable, BetweenStr, FormatDate, sep=''), recursive=TRUE)   #Search based on pattern given
  
  StartLoc = paste(MainSearchDir, Files, sep='')              #Prepare to copy over grib files to local directory
  LocalVariable = paste(LocalDir, Variable, '/', sep='')
  if(!dir.exists(LocalVariable)) dir.create(LocalVariable)
  file.copy(StartLoc, LocalVariable) #Specify full file name in "from" but only the directory in "to"
  
  
  
  LocalFiles = list.files(LocalVariable)                    #Get local files and append directory to get full file paths
  LocalPaths = paste(LocalVariable, LocalFiles, sep='')
  
  
  #Now to determine the exact files we want. The list is sorted properly, so that gives us a start.
  #0-192 Hours are in 3 hour intervals, and beyond that, it follows a 6 hour interval.
  
  #We can assume that there will always be more than 8 days until the end of the month.
  #Since Canada is roughly 6 hours offset from UTC, we will need to offset by 6 hours.
  
                                      #The actual hours - 6, 9, 12...
  
  #It will be different depending on which variable we're handling, so it's best to just do each one individually again.
  
  if(Variable == 'APCP_SFC_0'){
    FileCount = seq(1, as.integer(DaysInForecast) + 1, 1)   #We have a 6 hour and (24 * day) + 6 hour forecast 
    TimeCount = seq(6, (as.integer(DaysInForecast)*24 + 6), 24)  
  
    for(FileNum in 1:length(FileCount)){    #LocalPaths at FileNum in FileCount corresponds to Timecount at FileNum in FileCount
    
      if((TimeCount[FileNum]-6) %% 24 == 0){
        system(paste(InvokeWgrib, LocalPaths[FileCount[FileNum]], WgribArgs, TempFile))   #invoke wgrib to convert a grib file to nc
      
        Data = nc_open(TempFile)                  #the open function for ncdf4
      
        Lats = ncvar_get(Data, Data$dim$latitude)   #get a matrix of lats and of lons
        Lons = ncvar_get(Data, Data$dim$longitude)
        
        for(Member in 1:21){      #Get a lat x lon x member array of precip values
          
          PrecipArray[,,Member] = ncvar_get(Data, Data$var[[Member]])[361:720,181:361]  #forecast is at 0.5 degree resolution, so this takes lat 0-90 and lon 180-360 (a quarter of it)
      
        }
        
        #Precip is cumulative within a forecast, so precip at day 2 = precip at day 2 + day 1, precip at day 3 = 1 + 2 + 3, etc. This just does some subtraction to 
        
        if(FileNum != 1){
          DiffArray = PrecipArray - OldPrecipArray
        } else {
          DiffArray = PrecipArray
        }
        
        OldPrecipArray = PrecipArray
      
        nc_close(Data)
      }
      
      if(((TimeCount[FileNum]-6) / 24) != 0){                         #Assign to export array as long as it isn't day 0
        ExportArray[,,,((TimeCount[FileNum]-6) / 24),1] = DiffArray
      }
      
    }
    
    #The temperature process is virtually identical, but with dailymax and dailymin being used before assigning to exportarray
    
  } else if(Variable == 'TMP_TGL_2m'){
    
    if(DaysInForecast > 8){
      
      Hr192 = ((24 / 3) * 8) -1
      FileCount = c(seq(1, Hr192), 
                    seq((Hr192 + 1), Hr192 + 1 + (as.integer(DaysInForecast) - 8) * (24/6) -1))   #Painful to look at
      
      TimeCount = c(seq(6, 192,3),
                    seq(198, (198 + as.integer(DaysInForecast - 8)*24 - 6), 6))
      
    } else {
      
      FileCount = 1:(as.integer(DaysInForecast) * (24/3))
      
      TimeCount = seq(6, (as.integer(DaysInForecast) * 24)+3, 3)
      
    }
    
    TemperatureArray = array(NA, c(360, 181, 21, 8))
    TimeStepCount = 0
    for(FileNum in 1:length(FileCount)){    #LocalPaths at FileNum in FileCount corresponds to Timecount at FileNum in FileCount
      TimeStepCount = TimeStepCount + 1
      
      system(paste(InvokeWgrib, LocalPaths[FileCount[FileNum]], WgribArgs, TempFile))
      
      Data = nc_open(TempFile)
      
      Lats = ncvar_get(Data, Data$dim$latitude)
      Lons = ncvar_get(Data, Data$dim$longitude)
   
      for(Member in 1:21){
        
        TemperatureArray[,,Member,TimeStepCount] = ncvar_get(Data, Data$var[[Member]])[361:720,181:361]
      
        if((TimeCount[FileNum]-6) %% 24 == 0){      #Each day does 0 to 21 (or 0 to 18 if 6 hr steps)
      
          ExportArray[,,Member,ceiling((TimeCount[FileNum]-6) / 24), 2] = DailyMax(TemperatureArray[,,Member,])
          ExportArray[,,Member,ceiling((TimeCount[FileNum]-6) / 24), 3] = DailyMin(TemperatureArray[,,Member,])
        
        }
        
      }
      
      if((TimeCount[FileNum]-6) %% 24 == 0){
        #wipe temperature per day values
        TimeStepCount = 0
        TemperatureArray = array(NA, c(360, 181, 21, 8))   #Lat, Lon, Members, TimeSteps
      }
      
    }
    for(Member in 1:21){    #The last value is not a 24h value, so it will not hit the record and wipe parts, so we stick it in now
      ExportArray[,,Member,ceiling((TimeCount[FileNum]-6) / 24), 2] = DailyMax(TemperatureArray[,,Member,])
      ExportArray[,,Member,ceiling((TimeCount[FileNum]-6) / 24), 3] = DailyMin(TemperatureArray[,,Member,])
    }
  }
  

  
}

#All days and all members have been placed into ExportArray for precipitation and Tmax/Tmin. Now we just export each file.
if(!dir.exists(OutDir)) dir.create(OutDir)

for(FormatFile in FormatFiles){                   #Note that we scan through an already existing set of files named by lat lon so that we make a matching set
  OutFile = paste(OutDir, FormatFile, sep='')
  
  LatLon = strsplit(FormatFile, '.csv')[[1]]      #Generate location to extract the proper lat and lon from exportarray
  LatLon = unlist(strsplit(LatLon, '_'))
  
  write.csv(ExportArray[((as.numeric(LatLon[2]) - 180) * 2)+1, (as.numeric(LatLon[1]) * 2) + 1,,,], OutFile, quote=FALSE)
}
