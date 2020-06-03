#Prepare 24 Hour Forecast Gribs

#We currently have a forecast "backing" in \GriddedWeather\Forecast1719 which is composed of 24 hour forecasts and runs continually from June 1, 2017 onward. The goal is to be able to update this backing with new days. This will only make use of ensemble member 0.

if(!require(qmap)){
  install.packages('qmap')
}
if(!require(ncdf4)){
  install.packages('ncdf4')
}

library(ncdf4)      #library function is much like python's import
library(qmap)

#Custom functions. Called like Variable = Dailymax(ValuesArray). Takes an array of n values, and calculates a max, min, mean, or difference between the first and last. Depends on which function you call.

###########################################################################################################################################################

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

###########################################################################################################################################################


#Input of the date this is run on. Taken as a command line argument.
StartDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
StartDateFormat = format(StartDate, '%Y%m%d')
DateRange = 1

MainDODir = paste0(commandArgs(trailingOnly = TRUE)[3], '\\')

#Variable declarations - things like paths

FormatDir = paste0(MainDODir, '24HForecasts\\CSV\\')   #Since the files in here represent the location we want, we can list out all the files to find every lat,lon pair that we want to look at.
FormatFiles = list.files(FormatDir)
FormatLength = length(FormatFiles)

BetweenStr = '_latlon0p5x0p5_'    #It goes between the variable and the date components of the file name. Kinda dumb tbh.
MainSearchDir = paste0(MainDODir, '24HForecasts\\Grib/dd.weather.gc.ca/ensemble/geps/grib2/raw/00/')    #The folder where the forecasts are kept
InvokeWgrib = paste0(MainDODir, 'wgrib2/wgrib2')        #where my wgrib program is kept
WgribArgs = '-set_ext_name 1 -netcdf'   #arguments to make the wgrib output nice
TempFile = paste0(MainDODir, 'Temp/Temp.nc')  #A temporary nc file. We convert a grib to this file, read it, then delete it. Saves some disk space.

OutDir = paste0(MainDODir, '24HForecasts\\CSV\\')

TransformDir = paste0(MainDODir, 'Misc\\StatisticalTransforms\\')


Variables = c('APCP_SFC_0', 'TMP_TGL_2m')     #Precipitation and Temperature variables

ExportArray = array(-99, c(360,181,3))    #Lon, Lat, (Day, Precipitation, Tmax, Tmin)

InputArray = array(0, c(360,181))

#############

CurrentDate = StartDate                             #Set the current date as the starting date
StartDateFormat = format(CurrentDate, '%Y%m%d')


#Steps through each variable and each hour step in the data and assign it to the export array.

for(Variable in Variables){
  
  if(Variable == 'APCP_SFC_0'){                                     #Precip processing
    DataArray = array(0, c(360, 181, 2))      #Lon, Lat, Timesteps
    
    Hours = c('006','030')                    #Only take the first and last hours since we just need a daily total
    
    
    for(Hour in Hours){
      
      FileName = paste0(MainSearchDir, Hour, '\\', 'CMC_geps-raw_', Variable, BetweenStr, StartDateFormat, '00_P', Hour, '_allmbrs.grib2')
      
      system(paste(InvokeWgrib, FileName, WgribArgs, TempFile))
      
      Data = nc_open(TempFile)                  #the open function for ncdf4
      
      Lats = ncvar_get(Data, Data$dim$latitude)   #get a matrix of lats and of lons
      Lons = ncvar_get(Data, Data$dim$longitude)
      InputArray[,] = ncvar_get(Data, Data$var[[1]])[361:720,181:361]
      
      DataArray[,,which(Hours == Hour)] = InputArray
      
    }
    
    ExportArray[,,1] = DataArray[,,2] - DataArray[,,1]
    
  }else if(Variable == 'TMP_TGL_2m'){                               #Temperature processing
    
    Hours = c('006','009','012','015','018','021','024','027')
    DataArray = array(0, c(360, 181, 8))   #Lon, Lat, Timesteps
    
    for(Hour in Hours){
      
      FileName = paste0(MainSearchDir, Hour, '\\', 'CMC_geps-raw_', Variable, BetweenStr, StartDateFormat, '00_P', Hour, '_allmbrs.grib2')
      
      system(paste(InvokeWgrib, FileName, WgribArgs, TempFile))
      
      Data = nc_open(TempFile)                  #the open function for ncdf4
      
      Lats = ncvar_get(Data, Data$dim$latitude)   #get a matrix of lats and of lons
      Lons = ncvar_get(Data, Data$dim$longitude)
      InputArray[,] = ncvar_get(Data, Data$var[[1]])[361:720,181:361]
      
      DataArray[,,which(Hours == Hour)] = InputArray
      
    }
    
    ExportArray[,,2] = (DailyMax(DataArray) - 273.15)
    ExportArray[,,3] = (DailyMin(DataArray) - 273.15)
    
  }
  
  
  
}


DummyFile = paste0(OutDir, FormatFiles[1])                                    #Create a dummy to assign actual fitted values to
DummyData = read.csv(DummyFile)
DummyMap = fitQmapRQUANT(DummyData[,3], DummyData[,4], qstep=0.01, wet.day=TRUE, nboot=1)

#All values are assigned to ExportArray as Lon, Lat, Variable. Now to the exporting. Essentially we check which files exist, and split their name (they're named as Lat_Lon.csv) to find their location. This location represents the 1st and 2nd indices in the array - as in [Lon, Lat, ]. Then we export to that file with all three variables (tmax, tmin, precip).

for(FormatFile in FormatFiles){
  OutFile = paste(OutDir, FormatFile, sep='')
  
  if(!file.exists(OutFile)) next
  
  LatLon = strsplit(FormatFile, '.csv')[[1]]      #Generate location to extract the proper lat and lon from exportarray
  LatLon = unlist(strsplit(LatLon, '_'))
  
  TransformFile = paste0(TransformDir, FormatFile)
  
  TransformData = read.csv(TransformFile)
  rownames(TransformData) = NULL
  colnames(TransformData) = NULL
  TransformData = as.matrix(TransformData)
  
  CurrentPosition = StartDate
  Month = as.integer(format(CurrentPosition, format='%m'))
  MonthOffset = (Month - 1) * 5
  
  PrecipMap = DummyMap
  TempMap = DummyMap
  
  PrecipMap$wet.day = TransformData[MonthOffset + 1,1]
  TempMap$wet.day = NULL
  
  PrecipMap$par$modq[,1] = TransformData[MonthOffset + 2,]
  TempMap$par$modq[,1] = TransformData[MonthOffset + 4,]
  
  PrecipMap$par$fitq[,1] = TransformData[MonthOffset + 3,]
  TempMap$par$fitq[,1] = TransformData[MonthOffset + 5,]
  
  #PrecipMap$par$slope[,1] = TransformData[MonthOffset + 4,1:2]
  #TempMap$par$slope[,1] = TransformData[MonthOffset + 7,1:2]
  
  
  OldData = read.csv(OutFile)
  OldData$Date = as.Date(OldData$Date)      #Surprising, but yes it doesn't understand how to read its default date format
  
  Addon = as.data.frame(t(as.matrix(ExportArray[((as.numeric(LatLon[2]) - 180) * 2)+1, (as.numeric(LatLon[1]) * 2) + 1,])))   #Yes it really is that stupid. Thank you, R. You are such a great language.
  Addon = cbind(CurrentDate, Addon)
  colnames(Addon) = c('Date', 'Precip', 'TMax', 'TMin')
  
  Mean = (Addon$TMax + Addon$TMin)/2
  Range = (Addon$TMax - Addon$TMin)/2
  
  Addon$Precip = doQmap(Addon$Precip, PrecipMap)
  Temperature = doQmap(Mean, TempMap)
  Addon$TMax = Temperature + Range
  Addon$TMin = Temperature - Range
  
  NewData = rbind(OldData, Addon)
  
  write.csv(NewData, OutFile, quote=FALSE, row.names = FALSE)
}
