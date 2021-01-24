if(!require(ncdf4)){
  install.packages('ncdf4')
}

library(ncdf4)      #library function is much like python's import

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

RepeatWait = function(CheckFile){
  if(file.exists(CheckFile)){
    Sys.sleep(60)
    return(RepeatWait(CheckFile))
  } else {
    return(1)
  }
}

UserPauseFunction = function(){
  a1 = readline(prompt = 'Hit enter to continue')
  return(toString(a1))
}

ReadFromServer = function(){
  
  for(File in ServerFiles){
    ServerFile = paste0(ServerDir, File)
    LocalFile = paste0(LocalDir, File)
    ServerData = read.csv(ServerFile)
    LocalData = read.csv(LocalFile)
    
    LocalData = ServerData
    write.csv(LocalData, LocalFile, quote = FALSE, row.names = FALSE)
    
  }
  
}

WriteToServer = function(){
  
  for(File in ServerFiles){
    ServerFile = paste0(ServerDir, File)
    LocalFile = paste0(LocalDir, File)
    ServerData = read.csv(ServerFile)
    LocalData = read.csv(LocalFile)
    
    ServerData = LocalData
    write.csv(ServerData, ServerFile, quote = FALSE, row.names = FALSE)
    
  }
  
  UpdateString = paste0('Updated by ', UniqueID, ' at date ', CurrentDate)
  
  LogData = rbind(LogData, unlist(strsplit(UpdateString, ' ')))
  write.table(LogData, LogFile, quote=FALSE, row.names=FALSE, col.names = FALSE)

}

WriteLocalFiles = function(){
  for(Variable in Variables){
    
    if(Variable == 'APCP_SFC_0'){                                     #Precip processing
      DataArray = array(0, c(360, 181, 2))      #Lon, Lat, Timesteps
      
      Hours = c('006','030')                    #Only take the first and last hours since we just need a daily total
      
      
      for(Hour in Hours){
        
        FileName = paste0(MainSearchDir, Hour, '\\', 'CMC_geps-raw_', Variable, BetweenStr, CurrentDateFormat, '00_P', Hour, '_allmbrs.grib2')
        
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
        
        FileName = paste0(MainSearchDir, Hour, '\\', 'CMC_geps-raw_', Variable, BetweenStr, CurrentDateFormat, '00_P', Hour, '_allmbrs.grib2')
        
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
  
  #All values are assigned to ExportArray as Lon, Lat, Variable. Now to the exporting. Essentially we check which files exist, and split their name (they're named as Lat_Lon.csv) to find their location. This location represents the 1st and 2nd indices in the array - as in [Lon, Lat, ]. Then we export to that file with all three variables (tmax, tmin, precip).
  
  for(LocalFile in LocalFiles){
    OutFile = paste(LocalDir, LocalFile, sep='')
    
    if(!file.exists(OutFile)) next
    
    LatLon = strsplit(LocalFile, '.csv')[[1]]      #Generate location to extract the proper lat and lon from exportarray
    LatLon = unlist(strsplit(LatLon, '_'))
    
    OldData = read.csv(OutFile)
    OldData$Date = as.Date(OldData$Date)      #Surprising, but yes it doesn't understand how to read its default date format
    
    Addon = as.data.frame(t(as.matrix(ExportArray[((as.numeric(LatLon[2]) - 180) * 2)+1, (as.numeric(LatLon[1]) * 2) + 1,])))   #Yes it really is that stupid. Thank you, R. You are such a great language.
    Addon = cbind(CurrentDate, Addon)
    colnames(Addon) = c('Date', 'Precip', 'TMax', 'TMin')
    
    if(OldData[dim(OldData)[1],1] == CurrentDate) next   #This is in case we previously copied the file over from the network AND it was already updated to today.
    
    NewData = rbind(OldData, Addon)
    
    write.csv(NewData, OutFile, quote=FALSE, row.names = FALSE)
  }
}

CurrentDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
CurrentDateFormat = format(CurrentDate, '%Y%m%d')
MainDir = paste0(commandArgs(trailingOnly = TRUE)[3], '\\')
NetworkLocation = '\\\\skyemci01-eo.efs.agr.gc.ca\\projects\\droughtOutlook\\Operational\\'

LogFile = paste0(NetworkLocation, 'DailyLog.txt')
LoggingFile = paste0(NetworkLocation, 'CurrentlyLogging.txt')

LocalDir = paste0(MainDir, '24HForecasts\\CSV\\')
LocalFiles = list.files(LocalDir)
ServerDir = paste0(NetworkLocation, 'CSV\\')
ServerFiles = list.files(ServerDir)

User = Sys.info()['user']
Machine = Sys.info()['nodename']
UniqueID = paste0(User, '-', Machine)

Variables = c('APCP_SFC_0', 'TMP_TGL_2m')     #Precipitation and Temperature variables
ExportArray = array(-99, c(360,181,3))    #Lon, Lat, (Day, Precipitation, Tmax, Tmin)
InputArray = array(0, c(360,181))

BetweenStr = '_latlon0p5x0p5_'    #It goes between the variable and the date components of the file name. Kinda dumb tbh.
MainSearchDir = paste0(MainDir, '24HForecasts\\Grib/dd.weather.gc.ca/ensemble/geps/grib2/raw/00/')    #The folder where the forecasts are kept
InvokeWgrib = paste0(MainDir, 'wgrib2/wgrib2')        #where my wgrib program is kept
WgribArgs = '-set_ext_name 1 -netcdf'   #arguments to make the wgrib output nice
TempFile = paste0(MainDir, 'Temp/Temp.nc')  #A temporary nc file. We convert a grib to this file, read it, then delete it. Saves some disk space.

#First we want to make sure we have the same number and names of files
#This won't check the content - only the names
if(!(identical(LocalFiles, ServerFiles))){
  cat('! ERROR !')
  cat('Local and server backup files mismatch - Make sure neither are missing files first.')
  cat('Press enter to continue')
  test = readLines(con='stdin', 1)
  quit()
}

LoggingText = rbind('Records are currently being accessed. Please come back in a few minutes, and do not do ***ANYTHING*** to this file.', UniqueID)

if(file.exists(LoggingFile)){
  WhoIsUpdating = read.csv(LoggingFile, header = FALSE)[2,1]
  if(WhoIsUpdating == UniqueID){
    setwd(NetworkLocation)
    unlink(LoggingFile)
  } else {
    RepeatWait(LoggingFile)
  }
}

write.table(LoggingText, LoggingFile, row.names= FALSE, col.names = FALSE, quote=FALSE)

LogData = readLines(LogFile)
LogData = LogData[length(LogData)]
LastRecord = as.Date(unlist(strsplit(LogData, ' '))[length(unlist(strsplit(LogData, ' ')))])

SampleLocal = read.csv(paste0(LocalDir, LocalFiles[1]))
LastLocal = as.Date(SampleLocal[dim(SampleLocal)[1],1])

#Now we have the current date and the last record date for local and server files


if(LastRecord == CurrentDate){
  if(LastLocal == CurrentDate){
    
    #You're already up to date
    cat("Everything already up to date - You don't need to run this multiple times per day")
    cat('Press enter to continue')
    test = readLines(con='stdin', 1)
    
  } else if(LastLocal == (CurrentDate - 1)){
    
    #Archive is up to date but the local needs new data
    #run normally 
    WriteLocalFiles()
    
  } else if(LastLocal < (CurrentDate - 1)){
    
    #Archive is up to date but local is missing some files - just copy all missing from server and be done
    ReadFromServer()
    
  }
} else if(LastRecord == (CurrentDate - 1)){
  if(LastLocal == CurrentDate){
    
    #You're somehow up to date but the server is not
    #Update the server from local files but do not add to local
    
    WriteToServer()
    
  } else if(LastLocal == (CurrentDate - 1)){
    
    #Both local and server files are out one day
    #Run the local update and then use that to update the server
    
    WriteLocalFiles()
    WriteToServer()
    
  } else if(LastLocal < (CurrentDate - 1)){
    
    #Local files are serveral days old, but the server is updated to yesterday
    #first update to yesterday from the server
    #Next run the local file process
    #Finally update the server with the new data
    ReadFromServer()
    WriteLocalFiles()
    WriteToServer()
    
  }
} else if(LastRecord < (CurrentDate - 1)){
  if(LastLocal == CurrentDate){
    #You're somehow up to date but the server is not
    #Update the server from local files but do not add to local
    WriteToServer()
    
  } else if(LastLocal == (CurrentDate - 1)){
    
    #Both local and server files are out, the server more so than local
    #Run the local update and then use that to update the server
    WriteLocalFiles()
    WriteToServer()
    
  } else if(LastLocal < (CurrentDate - 1)){
    #Catastrophic failure - both local and server records are well out of date
    print('! Catastrophic Failure !')
    print('Server and local records are both outdated.')
    print('Check how out of date the records are - if it is 2 or 3 days, you can run this script and hardcode the past 2 days at most to build records back. If not, you will need to find another source for GEPS for the missing days and manually correct.')
    cat('Press enter to continue')
    test = readLines(con='stdin', 1)
    #quit()
    
  }
}

setwd(NetworkLocation)
unlink(LoggingFile)