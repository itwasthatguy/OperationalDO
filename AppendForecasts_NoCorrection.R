if(!require(doParallel)){
  install.packages('doParallel')
}
if(!require(foreach)){
  install.packages('foreach')
}
if(!require(qmap)){
  install.packages('qmap')
}

qs = function(vec){
  return(quantile(vec, seq(0.01,1,0.005)))
}

library(parallel)
library(doParallel)
library(foreach)
library(qmap)

clCount = detectCores() - 4

cl = makeCluster(clCount)                                            
registerDoParallel(cl)


#The short, or 24h forecasts come from one directory with only one member. Since 24h forecasts should be similar regardless of member, this is acceptable. Longer terms come from 21 directories, one for each member.

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
LastDay = as.Date(commandArgs(trailingOnly = TRUE)[3])
MainDir = paste0(commandArgs(trailingOnly = TRUE)[4], '\\')

FirstDate = as.Date('2017-06-01')                       #The first day in our record of forecasts.

ShortForecastDir = paste0(MainDir, '24HForecasts\\CSV\\')                    #Directory of short term forecasts in timeseries. This runs from FirstDate to ForecastDate.

LongForecastDir = paste0(MainDir, 'MonthlyForecasts\\', ForecastDate, 'CSV\\')  #Directory of long term forecasts which we have just taken out of the gribs
OutDir = paste0(MainDir, 'MonthlyForecasts\\', ForecastDate, 'Forecast\\')      #Output location

ShortForecastStart = as.Date("2017-06-01")    #June 1, 2017 (Same as FirstDate. Shhhhh.)
LongForecastStart = as.Date(ForecastDate)     #The first thursday of the month
LongForecastEnd = as.Date(LastDay)       #The last day of the same month

DaysUntilLong = as.integer(LongForecastStart - ShortForecastStart)    #Total number of short term forecast days already in timeseries.
TotalDays = as.integer(LongForecastEnd - ShortForecastStart) + 1      #Total number '' plus the long term forecast we're appending.

ShortForecastFiles = list.files(ShortForecastDir)
LongForecastFiles = list.files(LongForecastDir)

TransformDir = paste0(MainDir, 'Misc\\QM_GPD_Transforms\\')    #Pre-calibrated transforms to ensure our forecasts match the grid.
TransformFiles = list.files(TransformDir)

FormatFiles = intersect(TransformFiles, ShortForecastFiles)
FormatFiles = intersect(FormatFiles, LongForecastFiles)

dir.create(OutDir)

MonthDays = list()     #Create a list of 12 vectors - each of these 12 vectors will represent the days which are in this month.
for(Day in 1:TotalDays){
  CurrentMonth = format(FirstDate + Day - 1, format="%m")
  MonthDays[[CurrentMonth]] = append(MonthDays[[CurrentMonth]], Day)
}

#Hindcast data format - Lon field is 1 lower than the cell index - so 280 degrees would be cell 281
#                     - Lat field is 91 lower than the cell index - so 45 degrees would be cell 136

#Cells taken in the hincast go from Lon 211:330 and lat 131:180
#This means our 120x50 array has values from Lon 210 to 329 and Lat 40 to 89

#Index mapping - Lon = Lon Index + 209
#              - Lat = Lat Index + 39

#All bias correction calibrations

BCPrecip3 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Pcp_3.csv'))
BCMax3 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Max_3.csv'))
BCMin3 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Min_3.csv'))
BCPrecip4 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Pcp_4.csv'))
BCMax4 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Max_4.csv'))
BCMin4 = read.csv(paste0(MainDir, 'Misc\\BiasCorrections\\', ForecastDate, '\\BC_Min_4.csv'))

Month = as.numeric(format(LastDay, format = "%m"))


DummyFile = paste0(ShortForecastDir, FormatFiles[1])                                    #Create a dummy to assign actual fitted values to
DummyData = read.csv(DummyFile)
DummyMap = fitQmapRQUANT(DummyData[,3], DummyData[,4], qstep=0.01, wet.day=TRUE, nboot=1)

foreach(j = 1:40) %dopar%{      #Break the data up into 40 chunks and have a core handle each chunk. Each core only needs to hit a few hundred points as a result.
  
  library(qmap)
  
  Lower = floor(((j-1) * length(FormatFiles)/40) + 1)
  Upper = floor(((j) * length(FormatFiles)/40))
  
  for(i in Lower:Upper){
    
    ShortForecast = FormatFiles[i]
    LatLon = unlist(strsplit(unlist(strsplit(ShortForecast, '.csv'))[1], '_'))
    LatMap = floor(as.numeric(LatLon[1])+0.5) - 39
    LonMap = floor(as.numeric(LatLon[2])+0.5) - 209
    
    CombinedForecast = as.data.frame(array(0, c(TotalDays, 3))) #Raw data frame to put Precip, Tmax, and Tmin into
    
    LongPath = paste(LongForecastDir, ShortForecast, sep ='')  #The file name is the same, so with a different path, it works
    ShortPath = paste(ShortForecastDir, ShortForecast, sep = '')
    
    LongData = as.matrix(read.csv(LongPath))       #Data is in different formats between the two files
    
    ShortData = read.csv(ShortPath)[1:DaysUntilLong,]      #We want 24h forecasts until the start date of our longer forecast
    
    TransformFile = paste0(TransformDir, ShortForecast)
    TransformData = read.csv(TransformFile)[,1:101]
    
    rownames(TransformData) = NULL
    colnames(TransformData) = NULL
    TransformData = as.matrix(TransformData)
    
    for(Member in 1:21) {                #We'll want to go through for each member and create an independent forecast
      
      if(!exists(paste(OutDir, Member - 1, '\\', sep=''))) dir.create(paste(OutDir, Member - 1, '\\', sep=''))
      OutFile = paste(OutDir, Member - 1, '\\', ShortForecast, sep='')  #Last directory and file name for output
      
      LongMember = LongData[Member, 2:dim(LongData)[2]]   #Heh
      
      Length = length(LongMember)/3 #Since we have 3 forecast variables
      
      LongPrecip = as.vector(LongMember[1:Length])
      LongTMax = LongMember[(Length+1):(Length*2)] - 273.15       #Kelvin...
      LongTMin = LongMember[((Length*2)+1):(Length*3)] - 273.15
      
      #Apply bias correction to values at or beyond week 3. If precipitation becomes negative, bring it back to zero.
      
      for(ForecastDuration in 1:Length){
        if(ForecastDuration > 21){
          LongPrecip[ForecastDuration] = LongPrecip[ForecastDuration] + BCPrecip4[LonMap, LatMap]
          LongTMax[ForecastDuration] = LongTMax[ForecastDuration] + BCMax4[LonMap, LatMap]
          LongTMin[ForecastDuration] = LongTMin[ForecastDuration] + BCMin4[LonMap, LatMap]
        } else if(ForecastDuration > 14){
          LongPrecip[ForecastDuration] = LongPrecip[ForecastDuration] + BCPrecip3[LonMap, LatMap]
          LongTMax[ForecastDuration] = LongTMax[ForecastDuration] + BCMax3[LonMap, LatMap]
          LongTMin[ForecastDuration] = LongTMin[ForecastDuration] + BCMin3[LonMap, LatMap]
        }
        
        if(LongPrecip[ForecastDuration] < 0) LongPrecip[ForecastDuration] = 0
        
      }
      
      #With bias corrections done, now apply a transform to all values so that the distribution matches the gridded data
      
      CombinedForecast[1:DaysUntilLong,1] = ShortData[,2]
      CombinedForecast[(DaysUntilLong + 1):TotalDays,1] = LongPrecip
      
      CombinedForecast[1:DaysUntilLong,2] = ShortData[,3]
      CombinedForecast[(DaysUntilLong + 1):TotalDays,2] = LongTMax
      
      CombinedForecast[1:DaysUntilLong,3] = ShortData[,4]
      CombinedForecast[(DaysUntilLong + 1):TotalDays,3] = LongTMin
      
      PrecipMap = DummyMap
      TempMap = DummyMap
      
      MonthGroups = c('01','02','03','04','05','06','07','08','09','10','11','12')
      for(Month in MonthGroups){
        MonthOffset = (as.integer(Month) - 1) * 6
        InMonth = MonthDays[Month][[1]]
        
        PrecipMap$wet.day = TransformData[MonthOffset + 1,1]
        TempMap$wet.day = NULL
        
        PrecipMap$par$modq[,1] = TransformData[MonthOffset + 2,]
        TempMap$par$modq[,1] = TransformData[MonthOffset + 4,]
        
        PrecipMap$par$fitq[,1] = TransformData[MonthOffset + 3,]
        TempMap$par$fitq[,1] = TransformData[MonthOffset + 5,]
        
        ForecastTMean = (CombinedForecast[InMonth,2] + CombinedForecast[InMonth,3])/2
        ForecastRange = (CombinedForecast[InMonth,2] - CombinedForecast[InMonth,3]) / 2
        
        PreTF_Precip = CombinedForecast[InMonth,1]
        
        PrecipTransformed = rep(0, length(PreTF_Precip))
        PrecipTransformed = PreTF_Precip
        
        ParetoParams = TransformData[MonthOffset + 6,1:4]
        shape = ParetoParams[1]
        scale = ParetoParams[2]
        mshape = ParetoParams[3]
        mscale = ParetoParams[4]
        
        ExtremeVals = c()
        for(Val in PreTF_Precip[which(PreTF_Precip > qs(PreTF_Precip)[190])]){
          Probability = 1 - ((((mshape * Val)/mscale) + 1)^(-1/mshape))
          CorrectedVal = -1 * (scale * (((1-Probability)^shape) - 1) * (1 - Probability)^(-1 * shape)) / shape
          ExtremeVals = c(ExtremeVals, CorrectedVal)
        }
        
        PrecipTransformed = PreTF_Precip
        
        CombinedForecast[InMonth,1] = PrecipTransformed
        
        TempTransformed = ForecastTMean
        
        CombinedForecast[InMonth,2] = TempTransformed + ForecastRange
        CombinedForecast[InMonth,3] = TempTransformed - ForecastRange
      }
      
      #Transformed. Now merge it to the short forecast timeseries to create a full timeseries forecast until LastDay.
      
      
      #Append a date column spanning the entire duration
      
      DateSeq = seq(ShortForecastStart, LongForecastEnd ,'days')
      
      DatedForecast = cbind(DateSeq, CombinedForecast)
      
      colnames(DatedForecast) = c('Date', 'Precipitation', 'TMax', 'TMin')
      
      write.csv(DatedForecast, OutFile, quote=FALSE, row.names = FALSE)
      
    }
  }
  
}

stopCluster(cl)
