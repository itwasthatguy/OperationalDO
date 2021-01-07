#Delete last month's files

GoToThurs = function(Day){
  if(weekdays(Day) == 'Thursday'){
    return(Day)
  } else {
    return(GoToThurs(Day + 1))
  }
  
}

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
MainDirectory = paste0(commandArgs(trailingOnly = TRUE)[3], '\\')
DeleteCurrent = FALSE

ForecastMainDir = paste0(MainDirectory, 'MonthlyForecasts\\')
AccumulateMainDir = paste0(MainDirectory, 'MonthlyForecasts\\AccumulatedPrecip\\')
BCWeek1Dir = paste0(MainDirectory, 'Temp\\Week1\\')
BCWeek3Dir = paste0(MainDirectory, 'Temp\\Week3\\')
PDIDir = paste0(MainDirectory, 'Indices\\PDI\\')
SPIDir = paste0(MainDirectory, 'Indices\\SPI\\')
SPEIDir = paste0(MainDirectory, 'Indices\\SPEI\\')

AllDirs = c(ForecastMainDir, AccumulateMainDir, BCWeek1Dir, BCWeek3Dir, PDIDir, SPIDir, SPEIDir)

Year = as.integer(format(ForecastDate, format='%Y'))
Month = as.integer(format(ForecastDate, format='%m'))

PreviousYear = Year
PreviousMonth = Month - 1
if(PreviousMonth == 0){
  PreviousMonth = 12
  PreviousYear = PreviousYear - 1
}

if(PreviousMonth < 10) PreviousMonth = paste0('0', PreviousMonth)
PreviousDate = as.Date(paste0(PreviousYear, '-', PreviousMonth, '-01'))

PreviousDate = GoToThurs(PreviousDate)

#We now delete all previous month's folders entirely
#Option to delete current month's folders


for(DelDir in AllDirs){
  if(dir.exists(paste0(ForecastMainDir, DelDir))){
    setwd(DelDir)
    unlink(PreviousMonth, recursive=TRUE, force=TRUE)
  }
}

TarFiles = list.files(BCWeek1Dir, pattern = '.tar')
if(length(TarFiles > 0)){
  setwd(BCWeek1Dir)
  unlink(TarFiles)
}

TarFiles = list.files(BCWeek3Dir, pattern = '.tar')
if(length(TarFiles > 0)){
  setwd(BCWeek3Dir)
  unlink(TarFiles)
}

if(DeleteCurrent){
  
  for(DelDir in AllDirs){
    if(dir.exists(paste0(ForecastMainDir, DelDir))){
      setwd(DelDir)
      unlink(ForecastDate, recursive=TRUE, force=TRUE)
    }
  }
  
}