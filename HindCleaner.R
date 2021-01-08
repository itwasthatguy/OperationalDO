#Delete hindcast files after calculating bias corrections

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

BCWeek1Dir = paste0(MainDirectory, 'Temp\\Week1\\')
BCWeek3Dir = paste0(MainDirectory, 'Temp\\Week3\\')

AllDirs = c(BCWeek1Dir, BCWeek3Dir)

#We now delete all previous month's folders entirely
#Option to delete current month's folders


for(DelDir in AllDirs){
  if(dir.exists(DelDir)){
    setwd(DelDir)
    unlink(ForecastDate, recursive=TRUE, force=TRUE)
  }
}

TarFiles = list.files(BCWeek1Dir, pattern = '.tar')
if(length(TarFiles) > 0){
  setwd(BCWeek1Dir)
  unlink(TarFiles)
}

TarFiles = list.files(BCWeek3Dir, pattern = '.tar')
if(length(TarFiles) > 0){
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