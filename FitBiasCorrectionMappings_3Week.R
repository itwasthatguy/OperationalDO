#Generates the bias correction mappings for a forecast. Kept as a separate script because the downloading and extracting will take a long time to run, so it can be broken up a bit more conveniently.

#When a forecast is released, there is generally always going to be some bias in the model. This bias tends to increase as the time increases from the initialization. For our purposes, we have been advised to consider anything within the first 2 weeks to be more or less bias-free, and can be used without correction. Week 3 and week 4 will require some correction, done through a simple subtraction/addition.

#To find the bias, we will use the hindcast. For the thursday of our forecast, we will take the hindcast initialized at that same day, and extract the values at lead times of 3 weeks and 4 weeks. These weeks will be present for 1998 through 2017. As an example, if we were to forecast on August 1st, we would take the values hindcasted on August 1st of August 15th to 21st and August 22nd to 28th. From all these daily values of all years, we can create a mean daily value of whichever variable we're looking at. Next we will take the hindcasts initalized on those same weeks, so that we have the same daily values, only initialized at a lead time of 1 week instead of 3 and 4. From these, we also create our mean daily values. The difference between these values is our bias.

library(parallel)
library(doParallel)
library(foreach)
library(ncdf4)

#ForecastDate refers to the first day of the long term forecast, usually the first thursday of the month. Lastday refers to the last day of the month that the forecast ends in. MainDir is simply the main AutomaticDO directory.

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
LastDay = as.Date(commandArgs(trailingOnly = TRUE)[3])
MainDir = paste0(commandArgs(trailingOnly = TRUE)[4], '\\')

ShortForecastStart = as.Date("2017-06-01")    #June 1, 2017
LongForecastStart = as.Date(ForecastDate)     #It should already be as date, but this won't cause any harm
LongForecastEnd = as.Date(LastDay)

DaysUntilLong = as.integer(LongForecastStart - ShortForecastStart)  #Number of days in the time series before the extended forecast
TotalDays = as.integer(LongForecastEnd - ShortForecastStart) + 1    #And including the extended forecast

Week3 = ForecastDate + 14     #Week 3 vs week 4 and onwards

#Generates the url string to download with. It's just based on the date.

MMDD1 = paste0('http://collaboration.cmc.ec.gc.ca/cmc/ensemble/subX/hindcast/', format(ForecastDate, format='%m'), format(ForecastDate, format='%d'), '/')
MMDD3 = paste0('http://collaboration.cmc.ec.gc.ca/cmc/ensemble/subX/hindcast/', format(Week3, format='%m'), format(Week3, format='%d'), '/')


Vars = c('*tasmax_2m*','*tasmin_2m*','*pr_sfc*')    #The names of the variables. Maximum and minimum temperature, and precipitation.

dir.create(paste0(MainDir, 'Temp\\Week1\\', ForecastDate, '\\'))
dir.create(paste0(MainDir, 'Temp\\Week3\\', ForecastDate, '\\'))

for(Rep in 1:3){              #It's likely that the network will crap out at some point and we'll lose a file or two. This will re-run all missing archives so we'll probably be good. 3 Is arbitrary, but it should work well.
  for(Year in 1998:2017){
    for(Member in 1:4){
      
      #These sections go through and download the hindcast - given as zipped archives containing all variables. We then extract the variables we want, and place the files into another directory. We go through for weeks 1, 3 and 4, for all members, for all years.
      
      ArchiveString = paste0('subX_reforecast_ECCC_', Year, format(ForecastDate, format='%m'), format(ForecastDate, format='%d'), "00_m0", Member, '.tar')
      if(!file.exists(paste0(MainDir, 'Temp\\Week1\\', ArchiveString))){
        WgetString = paste0(MMDD1, ArchiveString)
        CmdString = paste0(MainDir, '\\Processes\\wget.exe -r -p -nH --cut-dirs 6 -np -P ', MainDir, 'Temp\\Week1\\ robots=off ', WgetString)
        system(CmdString)
        
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week1\\', ArchiveString, ' ', Vars[1], ' ', MainDir, 'Temp\\Week1\\', ForecastDate)
        system(RarString)
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week1\\', ArchiveString, ' ', Vars[2], ' ', MainDir, 'Temp\\Week1\\', ForecastDate)
        system(RarString)
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week1\\', ArchiveString, ' ', Vars[3], ' ', MainDir, 'Temp\\Week1\\', ForecastDate)
        system(RarString)
      }
      ArchiveString = paste0('subX_reforecast_ECCC_', Year, format(ForecastDate, format='%m'), format(Week3, format='%d'), "00_m0", Member, '.tar')
      if(!file.exists(paste0(MainDir, 'Temp\\Week3\\', ArchiveString))){
        WgetString = paste0(MMDD3, ArchiveString)
        CmdString = paste0(MainDir, '\\Processes\\wget.exe -r -p -nH --cut-dirs 6 -np -P ', MainDir, 'Temp\\Week3\\ robots=off ', WgetString)
        system(CmdString)
        
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week3\\', ArchiveString, ' ', Vars[1], ' ', MainDir, 'Temp\\Week3\\', ForecastDate)
        system(RarString)
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week3\\', ArchiveString, ' ', Vars[2], ' ', MainDir, 'Temp\\Week3\\', ForecastDate)
        system(RarString)
        RarString = paste0('winrar e -inul ', MainDir, 'Temp\\Week3\\', ArchiveString, ' ', Vars[3], ' ', MainDir, 'Temp\\Week3\\', ForecastDate)
        system(RarString)
      }
    }
  }
}

Week1Array = array(0, c(120,50,14,80,3))      #Lon, lat, day, year/mem - Week 1 needs to contain values for week 3 and 4 so it can compare both
Week3Array = array(0, c(120,50,7,80,3))      #Lon, lat, day, year/mem
Week4Array = array(0, c(120,50,7,80,3))      #Lon, lat, day, year/mem

for(Variable in 1:3){
  
  Files = list.files(paste0(MainDir, 'Temp\\Week1\\', ForecastDate, '\\'), pattern = Vars[Variable], full.names=TRUE)
  
  #Read in Week 1 at a lead time corresponding to weeks 3 and 4.
  
  Pos = 0
  for(File in Files){
    Pos = Pos + 1
    OpenFile = nc_open(File)
    OpenData = ncvar_get(OpenFile, OpenFile$var[[1]]$name)[211:330, 131:180, 15:28]
    Week1Array[,,,Pos,Variable] = OpenData
    nc_close(OpenFile)
  }
  
  Files = list.files(paste0(MainDir, 'Temp\\Week3\\', ForecastDate, '\\'), pattern = Vars[Variable], full.names=TRUE)
  
  #Read in week 3 at no lead time
  
  Pos = 0
  for(File in Files){
    Pos = Pos + 1
    OpenFile = nc_open(File)
    OpenData = ncvar_get(OpenFile, OpenFile$var[[1]]$name)[211:330, 131:180, 1:7]
    Week3Array[,,,Pos,Variable] = OpenData
    nc_close(OpenFile)
  }
  
  Files = list.files(paste0(MainDir, 'Temp\\Week3\\', ForecastDate, '\\'), pattern = Vars[Variable], full.names=TRUE)
  
  #And again with week 4
  
  Pos = 0
  for(File in Files){
    Pos = Pos + 1
    OpenFile = nc_open(File)
    OpenData = ncvar_get(OpenFile, OpenFile$var[[1]]$name)[211:330, 131:180, 8:14]
    Week4Array[,,,Pos,Variable] = OpenData
    nc_close(OpenFile)
  }
  
}

Week1Mean = array(0, c(120,50,2,3))      #Lon, lat, day, year/mem - Week 1 needs to contain values for week 3 and 4 so it can compare both
Week3Mean = array(0, c(120,50,3))      #Lon, lat, day, year/mem
Week4Mean = array(0, c(120,50,3))      #Lon, lat, day, year/mem

#Go through location by location and generate a weekly mean. For week 1, this results in two means, corresponding to week 3 and to week 4.

for(x in 1:120){
  for(y in 1:50){
    for(Variable in 1:3){
      Week1Mean[x,y,1,Variable] = mean(Week1Array[x,y,1:7,,Variable])
      Week1Mean[x,y,2,Variable] = mean(Week1Array[x,y,8:14,,Variable])
      Week3Mean[x,y,Variable] = mean(Week3Array[x,y,,,Variable])
      Week4Mean[x,y,Variable] = mean(Week4Array[x,y,,,Variable])
    }
  }
}

Week3Bias = array(0, c(120,50,3))
Week4Bias = array(0, c(120,50,3))

#Now we take the difference in week 3 and week 1 offset to week 3, as well as week 4 and week 1 offset to week 4. This gives us our bias.

for(x in 1:120){
  for(y in 1:50){
    Week3Bias[x,y,1] = (Week3Mean[x,y,1] - Week1Mean[x,y,1,1])
    Week4Bias[x,y,1] = (Week4Mean[x,y,1] - Week1Mean[x,y,2,1])
    
    Week3Bias[x,y,2] = (Week3Mean[x,y,2] - Week1Mean[x,y,1,2])
    Week4Bias[x,y,2] = (Week4Mean[x,y,2] - Week1Mean[x,y,2,2])
    
    Week3Bias[x,y,3] = (Week3Mean[x,y,3] - Week1Mean[x,y,1,3]) * 1000
    Week4Bias[x,y,3] = (Week4Mean[x,y,3] - Week1Mean[x,y,2,3]) * 1000
  }
}

#Now we can merge it into an array, and export the biases as CSV to be used later.

MergedBias = array(0, c(120,50,3,2))
MergedBias[,,,1] = Week3Bias
MergedBias[,,,2] = Week4Bias

dir.create(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate))

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Max_3.csv')
write.csv(MergedBias[,,1,1], Out, quote=FALSE, row.names=FALSE)

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Max_4.csv')
write.csv(MergedBias[,,1,2], Out, quote=FALSE, row.names=FALSE)

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Min_3.csv')
write.csv(MergedBias[,,2,1], Out, quote=FALSE, row.names=FALSE)

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Min_4.csv')
write.csv(MergedBias[,,2,2], Out, quote=FALSE, row.names=FALSE)

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Pcp_3.csv')
write.csv(MergedBias[,,3,1], Out, quote=FALSE, row.names=FALSE)

Out = paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Pcp_4.csv')
write.csv(MergedBias[,,3,2], Out, quote=FALSE, row.names=FALSE)
