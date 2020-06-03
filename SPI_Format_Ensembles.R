#This script basically just sums monthly precipitation and formats in the way that richard's SPI program likes. Overall it's fairly simple, going day by day and adding precipitation. When it reaches a new month, it resets.
if(!require(doParallel)){
  install.packages('doParallel')
}
if(!require(foreach)){
  install.packages('foreach')
}

library(parallel)
library(doParallel)
library(foreach)

elapsed_months <- function(end_date, start_date) {        #Thank you, pbnelson from stackoverflow
  ed <- as.POSIXlt(end_date)
  sd <- as.POSIXlt(start_date)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

#HowManyDays is called at the start of each month to determine the number of days in it (for aggregation to month purposes)
#Yes there are better ways to do this, but I wrote this when I was just starting with R.
HowManyDays = function(Year, Month){
  
  if(Month == 1 || Month == 3 || Month == 5 || Month == 7 || Month == 8 || Month == 10 || Month == 12){
    return(31)
  } else if(Month == 4 || Month == 6 || Month == 9 || Month == 11){
    return(30)
  } else {				#February...
    if(as.integer(Year) %% 400 == 0){
      return(29)
    } else if(as.integer(Year) %% 100 == 0){
      return(28)
    } else if(as.integer(Year) %% 4 == 0){
      return(29)
    } else {
      return(28)
    }
  }
}

#The first month defined as the end of the first month
#Command line arguments from the batch file as well...

StartDate = as.Date('2017-06-01')
StartMonth = as.Date('2017-06-30')
ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
LastDay = as.Date(commandArgs(trailingOnly = TRUE)[3])
MainDir = paste0(commandArgs(trailingOnly = TRUE)[4], '\\')

#Total number of months and days in the entirety of the timeseries - not just the forecast month

SetMonths = elapsed_months(LastDay, StartMonth) + 1
SetDays = LastDay - StartDate + 1

FirstYear = as.integer(format(StartDate, format = "%Y"))			#The date of our first data point
FirstMonth = as.integer(format(StartDate, format = "%m"))
FirstDay = as.integer(format(StartDate, format = "%d"))

PrimaryFilesDir = paste0(MainDir, "MonthlyForecasts\\", ForecastDate, "Forecast\\")     #Forecast directory

OutDir = paste0(MainDir, "MonthlyForecasts\\AccumulatedPrecip\\", ForecastDate, "\\")
if(!dir.exists(OutDir)) dir.create(OutDir)

Ensembles = array('', c(21))
for(i in 0:20){
  Ensembles[i+1] = paste0(PrimaryFilesDir, i, '\\')
}

############################################################################################################################################################

clCount = detectCores() - 4

cl = makeCluster(clCount)		
registerDoParallel(cl)

foreach(Member = 0:20) %dopar% {    #21 members, 21 cores.
  
  
  GridFiles = list.files(paste0(PrimaryFilesDir, Member, '\\'))
  for(File in GridFiles){				#Again, the file name is the same between grid and hind - all we need to do is choose the directory

    YearCount = FirstYear				#Initiailize the date
    MonthCount = FirstMonth
    DayCount = FirstDay
    
    TotalMonthCount = 1				#Will tick up each month
    OffsetDayCount = 1
    
    MonthLen = HowManyDays(YearCount, MonthCount) #Number of days in the month
    
    OutArray = array(0, c(SetMonths,3))
    
    Data = array(0, c(SetDays))
    NewFile = File
    
    for(TotalDayCount in 1:SetDays){		#Go through every day
      
      if(TotalDayCount == 1){
        Data = read.csv(paste(PrimaryFilesDir, Member, '\\', File, sep=""), header=TRUE)[,2]		#We know that we want to read from the grid file first...
      }
      
      
      if(DayCount == 1){                                #First day of the month - get the number of days
        MonthLen = HowManyDays(YearCount, MonthCount)
      }
      
      OutArray[TotalMonthCount,1] = YearCount
      OutArray[TotalMonthCount,2] = MonthCount
      
      #Replace NAs with 0
      if(is.na(Data[OffsetDayCount])){
        Data[OffsetDayCount] = 0
      }
      
      #And only include positives to the sum. Any errors in the data resulting in negatives are counted as zero.
      if(Data[OffsetDayCount] >= 0){
        OutArray[TotalMonthCount,3] = OutArray[TotalMonthCount,3] + Data[OffsetDayCount]
      }
      
      if(DayCount == MonthLen){     #Last day of the month
        
        DayCount = 1
        TotalMonthCount = TotalMonthCount + 1	#TotalDayCount handled in for loop...
        OffsetDayCount = OffsetDayCount + 1
        
        if(MonthCount == 12){               #Check to see if year ticks up and month goes back to 1
          MonthCount = 1
          YearCount = YearCount + 1
        } else {
          MonthCount = MonthCount + 1
        }
      } else {
        DayCount = DayCount + 1
        OffsetDayCount = OffsetDayCount + 1
      }
    }
    
    #Export
    
    if(!dir.exists(paste(OutDir, Member,sep=''))) dir.create(paste(OutDir, Member,sep=''))
    
    write.table(OutArray, paste(OutDir, Member, '\\', NewFile, sep=""), quote=FALSE, col.names=FALSE, row.names=FALSE, sep=" ")
  }
}

stopCluster(cl)
