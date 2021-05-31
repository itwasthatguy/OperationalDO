#Generate bias correction mappings on a month by month basis
#Richard pointed out that there's different bias by month. Additionally, we're bad and we're using multiple models, so this will mitigate.
#Given that SPI, SPEI, etc are calculated by the particular month, it's important that we ensure our data is correct for that particular month

library(qmap)
library(parallel)
library(doParallel)
library(foreach)

GridDir = 'D:\\Work\\AutomaticDO\\Historical\\HalfGrid5015\\' #Location of your daily gridded data

HindDirPcp = array("", c(4))
HindDirPcp[1] = 'D:\\Work\\AutomaticDO\\Historical\\PrecipHind\\1\\' #Location of hindcast data in CSV format - same format as the gridded data where 1 file =
HindDirPcp[2] = 'D:\\Work\\AutomaticDO\\Historical\\PrecipHind\\2\\' #1 location.
HindDirPcp[3] = 'D:\\Work\\AutomaticDO\\Historical\\PrecipHind\\3\\'
HindDirPcp[4] = 'D:\\Work\\AutomaticDO\\Historical\\PrecipHind\\4\\'

HindDirTmp = array("", c(4))
HindDirTmp[1] = 'D:\\Work\\AutomaticDO\\Historical\\TempHind\\1\\'
HindDirTmp[2] = 'D:\\Work\\AutomaticDO\\Historical\\TempHind\\2\\'
HindDirTmp[3] = 'D:\\Work\\AutomaticDO\\Historical\\TempHind\\3\\'
HindDirTmp[4] = 'D:\\Work\\AutomaticDO\\Historical\\TempHind\\4\\'

OutCalDir = 'D:\\Work\\AutomaticDO\\Misc\\StatisticalTransforms2\\'
dir.create(OutCalDir)

GridStart = as.Date('1950-01-01')   #There is no GridEnd because HindEnd comes first - we can just end there.

HindStart = as.Date("1998-01-03")   #These we have to just know
HindEnd = as.Date("2015-01-02")

AllFiles = list.files(GridDir)
FileCount = length(AllFiles)

AllDaysHind = HindEnd-HindStart + 1
AllDaysGrid = HindEnd-GridStart + 1

MonthDaysHind = list()     #Create a list of 12 vectors - each of these 12 vectors will represent the days which are in this month.
for(Day in 1:AllDaysHind){
  CurrentMonth = format(HindStart + Day - 1, format="%m")
  MonthDaysHind[[CurrentMonth]] = append(MonthDaysHind[[CurrentMonth]], Day)
}

MonthDaysGrid = list()     #Create a list of 12 vectors - each of these 12 vectors will represent the days which are in this month.
for(Day in 1:AllDaysGrid){
  CurrentMonth = format(GridStart + Day - 1, format="%m")
  MonthDaysGrid[[CurrentMonth]] = append(MonthDaysGrid[[CurrentMonth]], Day)
}

cl = makeCluster(4)			#If you for some reason have a 1 core cpu, this will cause problems. I suggest you get a better computer.
registerDoParallel(cl)

AllMap = foreach(i=1:20)%dopar%{
  library(qmap)  
  
  Lower = floor((i-1) * (FileCount/20)) + 1
  Upper = floor((i) * (FileCount/20))
  
  SubFilesList = (Lower:Upper)
  #InstMap = array(0, c(84, 101, length(SubFilesList)))
  jCounter = 0
  for(j in SubFilesList){
    jCounter = jCounter + 1
    
    LatLon = unlist(strsplit(AllFiles[j], '.csv'))[1]
    Lat = round(as.integer(unlist(strsplit(LatLon, '_'))[1]))
    Lon = round(as.integer(unlist(strsplit(LatLon, '_'))[2]))
    
    HindFile = paste0(Lat, '_', Lon, '.csv')
    
    if(!file.exists(paste0(HindDirPcp[1], HindFile))) next
    
    HindData = array(0, c(AllDaysHind, 12))
    HindData[,1] = read.csv(paste0(HindDirPcp[1], HindFile))[,2]
    HindData[,2] = read.csv(paste0(HindDirPcp[2], HindFile))[,2]
    HindData[,3] = read.csv(paste0(HindDirPcp[3], HindFile))[,2]
    HindData[,4] = read.csv(paste0(HindDirPcp[4], HindFile))[,2]
    
    HindData[,5] = read.csv(paste0(HindDirTmp[1], HindFile))[,2]
    HindData[,6] = read.csv(paste0(HindDirTmp[2], HindFile))[,2]
    HindData[,7] = read.csv(paste0(HindDirTmp[3], HindFile))[,2]
    HindData[,8] = read.csv(paste0(HindDirTmp[4], HindFile))[,2]
    
    GridData = read.csv(paste0(GridDir, AllFiles[j]))
    
    GridFittingPeriod = seq(which(as.Date(GridData[,1]) == GridStart), which(as.Date(GridData[,1]) == HindEnd))
    
    ObsPrecip = GridData$Precipitation[GridFittingPeriod]
    ObsTemp = GridData$TMax[GridFittingPeriod]
    ObsTemp = (ObsTemp + GridData$TMin[GridFittingPeriod])/2
    
    MonthsMap = array(0, c(12*7, 101))
    
    for(Month in 1:12){
      ObsMonth = ObsPrecip[MonthDaysGrid[[Month]]]
      HindMonth = HindData[MonthDaysHind[[Month]], 1:4]
      
      CountRows = dim(HindMonth)[1]                                         #Because rbind is dumb
      HindMonthCombine = array(0, c(CountRows * 4))
      HindMonthCombine[1:CountRows] = HindMonth[,1]
      HindMonthCombine[(CountRows + 1):(CountRows*2)] = HindMonth[,1]
      HindMonthCombine[((2 *CountRows) + 1):(CountRows*3)] = HindMonth[,1]
      HindMonthCombine[((3 *CountRows) + 1):(CountRows*4)] = HindMonth[,1]
      
      #HindMonthCombine = as.vector(HindMonthCombine[which(!is.na(HindMonthCombine))])
      HindMonthCombine[which(is.na(HindMonthCombine))] = 0
      HindMonthCombine[which(HindMonthCombine < 0)] = 0
      ObsMonth = as.vector(ObsMonth)
      
      Map = fitQmapRQUANT(ObsMonth, HindMonthCombine, qstep=0.01, wet.day=TRUE, nboot=1)
      
      MonthsMap[((Month-1)*7) + 1,] = Map$wet.day
      MonthsMap[((Month-1)*7) + 2,] =  Map$par$modq
      MonthsMap[((Month-1)*7) + 3,] =  Map$par$fitq
      MonthsMap[((Month-1)*7) + 4,1:2] =  Map$par$slope
      
      ObsMonth = ObsTemp[MonthDaysGrid[[Month]]]
      HindMonth = HindData[MonthDaysHind[[Month]], 5:8] - 273.15
      
      
      CountRows = dim(HindMonth)[1]                                         #Because rbind is dumb
      HindMonthCombine = array(0, c(CountRows * 4))
      HindMonthCombine[1:CountRows] = HindMonth[,1]
      HindMonthCombine[(CountRows + 1):(CountRows*2)] = HindMonth[,1]
      HindMonthCombine[((2 *CountRows) + 1):(CountRows*3)] = HindMonth[,1]
      HindMonthCombine[((3 *CountRows) + 1):(CountRows*4)] = HindMonth[,1]
      
      HindMonthCombine = as.data.frame(HindMonthCombine)
      ObsMonth = as.data.frame(ObsMonth)
      
      
      Map = fitQmapRQUANT(ObsMonth, HindMonthCombine, qstep=0.01, wet.day=FALSE, nboot=1)
      
      MonthsMap[((Month-1)*7) + 5,] =  Map$par$modq
      MonthsMap[((Month-1)*7) + 6,] =  Map$par$fitq
      MonthsMap[((Month-1)*7) + 7,1:2] =  Map$par$slope
      
    }
    
    
    LatLon = unlist(strsplit(HindFile, '.csv'))[1]
    LatLon = unlist(strsplit(LatLon, "_"))
    
    write.csv(MonthsMap, paste0(OutCalDir, AllFiles[j]), quote=FALSE, row.names=FALSE)
    
  }
  return(MonthsMap)
}

stopCluster(cl)


