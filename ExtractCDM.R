#This script downloads and reads the CDM for the *previous* month. This is because we use previous drought as a covariate. Most of this is just month and year calculation, and then calling wget and winrar.

#Command line arguments from the batch file. Dates and directories specified. Unlike usual, the forecast date is the last day of the month, rather than the usual first thursday. This is just for the off chance that the true "forecast date" is a late day in the previous month.

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
LastDate = as.Date(commandArgs(trailingOnly = TRUE)[3])
MainDir = commandArgs(trailingOnly = TRUE)[4]

library(sf)

#Paths

strCDMDir = paste0(MainDir, '\\CDM\\')
strPtsTemplatePath = paste0(MainDir, '\\Misc\\HalfGridPoints.csv')
strOutputDir = paste0(MainDir,'\\Indices\\CDM_Previous\\', ForecastDate,'\\')
URL = 'http://www.agr.gc.ca/atlas/data_donnees/cli/canadianDroughtMonitor/shp/areasOfDrought'

#Extract month and year as numeric values

intYear = as.integer(format(LastDate, format = "%y"))
intMonth = as.integer(format(LastDate, format = "%m"))
FullYear =  as.integer(format(LastDate, format = "%Y"))

#Reduce the month by one

if(intMonth == 1){
  intMonth = 12
  intYear = intYear - 1
  FullYear = FullYear - 1
} else {
  intMonth = intMonth - 1
}

#Two digits

if(intYear < 10){
  if(intMonth < 10){
    strDateFolder = paste0('0', intYear, '0', intMonth)
  } else {
    strDateFolder = paste0('0', intYear, intMonth)
  }
} else {
  if(intMonth < 10){
    strDateFolder = paste0(intYear, '0', intMonth)
  } else {
    strDateFolder = paste0(intYear, intMonth)
  }
}

#Generate strings to which file to download and which directory to place it

File=paste0(URL,'/',FullYear,'/cdm_',strDateFolder,'_drought_areas_shp.zip')
OutDir=paste0(MainDir, '\\CDM\\',strDateFolder,'\\')

if(!dir.exists(OutDir)) dir.create(OutDir)

#Opens the command prompt and enters these commands. Make sure you have wget in the right location, and make sure winrar is installed.

system(paste0(MainDir, '\\wget.exe -r -p -np -nH --cut-dirs=7 -P ',OutDir,' robots=off ',File))
  
system(paste0('winrar e ',OutDir,'cdm_',strDateFolder,'_drought_areas_shp.zip ',OutDir,'Unzip\\'))

#Read the shape file and extract the values at the locations given by the template CSV.

strCDMDir = paste0(strCDMDir, strDateFolder, '\\Unzip\\')

strShpFiles = list.files(strCDMDir, pattern = '\\.shp$', full.names=TRUE)

PtsTemplate = read.csv(strPtsTemplatePath)
PtsTemplate[,3] = PtsTemplate[,3] - 360
PtsTemplateGeo = st_as_sf(PtsTemplate, coords =c('V2', 'V1'), crs=4326)

intCDMVals = array(0, c(dim(PtsTemplate)[1]))

for(i in 1:length(strShpFiles)){
  strShpFile = strShpFiles[i]
  ShpData = read_sf(strShpFile)
  ShpDataTF = st_transform(ShpData, st_crs(PtsTemplateGeo))
  ShpIntersects = st_intersection(ShpDataTF, PtsTemplateGeo)
  
  intCDMVals[ShpIntersects$X] = i
}

PtsDroughtOut = cbind(PtsTemplate, intCDMVals)
PtsDroughtOut[,3] = PtsDroughtOut[,3] + 360

dir.create(strOutputDir)
write.csv(PtsDroughtOut, paste0(strOutputDir, 'CDM.csv'), row.names = FALSE)