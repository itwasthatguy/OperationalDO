#Apply statistical transformation to all days in a continuous record of daily values.

library(qmap)
library(parallel)
library(doParallel)
library(foreach)

StartDate = as.Date('2017-06-01')
EndDate = as.Date('2020-03-02')
InDir = 'F:\\AutomaticDO\\24HForecasts\\CSV\\'
OutDir = 'E:\\AutomaticDO\\24HForecasts\\CSV\\'
TFDir = 'E:\\AutomaticDO\\Misc\\StatisticalTransforms\\'

InFiles = list.files(InDir)
TFFiles = list.files(TFDir)
Files = intersect(InFiles, TFFiles)

DateRange = EndDate - StartDate + 1
MonthDays = list()     #Create a list of 12 vectors - each of these 12 vectors will represent the days which are in this month.
for(Day in 1:DateRange){
  CurrentMonth = format(StartDate + Day - 1, format="%m")
  MonthDays[[CurrentMonth]] = append(MonthDays[[CurrentMonth]], Day)
}

DummyFile = paste0(InDir, InFiles[1])                                    #Create a dummy to assign actual fitted values to
DummyData = read.csv(DummyFile)
DummyMap = fitQmapRQUANT(DummyData[,3], DummyData[,4], qstep=0.01, wet.day=TRUE, nboot=1)

cl = makeCluster(20)		
registerDoParallel(cl)

foreach(j = 1:40) %dopar%{  
  
  library(qmap)
  
  Lower = floor(((j-1) * length(Files)/40) + 1)
  Upper = floor(((j) * length(Files)/40))
  
  for(FileCount in Lower:Upper){
    
    File = Files[FileCount]
    
    Data = read.csv(paste0(InDir, File))
    TFData = read.csv(paste0(TFDir, File))[,1:101]
    
    rownames(TFData) = NULL
    colnames(TFData) = NULL
    TFData = as.matrix(TFData)
    
    OutData = Data
    
    for(Date in 1:DateRange){
      
      CurrentPosition = StartDate + Date - 1
      Month = as.integer(format(CurrentPosition, format='%m'))
      MonthOffset = (Month - 1) * 5
      
      PrecipMap = DummyMap
      TempMap = DummyMap
      
      PrecipMap$wet.day = TFData[MonthOffset + 1,1]
      TempMap$wet.day = NULL
      
      PrecipMap$par$modq[,1] = TFData[MonthOffset + 2,]
      TempMap$par$modq[,1] = TFData[MonthOffset + 4,]
      
      PrecipMap$par$fitq[,1] = TFData[MonthOffset + 3,]
      TempMap$par$fitq[,1] = TFData[MonthOffset + 5,]
      
      
      Range = (Data[Date,3] - Data[Date,4]) / 2
      Mean = (Data[Date,3] + Data[Date,4]) / 2
      
      OutData[Date,2] = doQmap(Data[Date,2], PrecipMap)
      OutData[Date,3] = doQmap(Mean, TempMap) + Range
      OutData[Date,4] = doQmap(Mean, TempMap) - Range
      
      
      
    }
    
    write.csv(OutData, paste0(OutDir, File), row.names = FALSE, quote = FALSE)
    
  }
}

stopCluster(cl)