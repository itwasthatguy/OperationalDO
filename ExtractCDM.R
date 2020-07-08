#This script downloads and reads the CDM for the *previous* month. This is because we use previous drought as a covariate. Most of this is just month and year calculation, and then calling wget and winrar.

#Command line arguments from the batch file. Dates and directories specified. Unlike usual, the forecast date is the last day of the month, rather than the usual first thursday. This is just for the off chance that the true "forecast date" is a late day in the previous month.

#print("This script doesn't work yet")
#q()

library(sf)

#CDMArchive = 'F:\\Location\\'

#ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
#LastDate = as.Date(commandArgs(trailingOnly = TRUE)[3])
#MainDir = commandArgs(trailingOnly = TRUE)[4]

ForecastDate = as.Date('2020-07-02')
LastDate = as.Date('2020-07-31')
MainDir = 'D:\\Work\\AutomaticDO\\'



#Paths

strCDMDir = paste0(MainDir, '\\CDM\\')
strPtsTemplatePath = paste0(MainDir, '\\Misc\\PointLocations.csv')
strOutputDir = paste0(MainDir,'\\Indices\\CDM_Previous\\', ForecastDate,'\\')

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


#Something to grab the file from the network


###

ZipDir = paste0(MainDir, 'CDM\\', strDateFolder, '\\')

if(!dir.exists(ZipDir)) dir.create(ZipDir)


####

#Once I know the directory and how it's saved, copy the zip file into ZipDir and we can carry on.

####

ExistingFiles = length(list.files(ZipDir))

if(ExistingFiles > 0){
  system(paste0('winrar e ',ZipDir,'CDM_',strDateFolder,'_lr_shp.zip ',ZipDir,'Unzip\\'))
} else {
  print('Shapefiles already present - check the date')
  
}

#Read the shape file and extract the values at the locations given by the template CSV.

strCDMDir = paste0(strCDMDir, strDateFolder, '\\Unzip\\')

strShpFiles1 = list.files(strCDMDir, pattern = '\\.shp$', full.names=TRUE)
#strShpFiles2 = list.files(strCDMDir, pattern = 'draft', full.names=TRUE)
#strShpFilesInt = intersect(strShpFiles1, strShpFiles2)
strShpFilesInt = strShpFiles1

PtsTemplate = read.csv(strPtsTemplatePath)
PtsTemplate[,2] = PtsTemplate[,2] - 360
PtsTemplate = cbind(1:dim(PtsTemplate)[1], PtsTemplate)
colnames(PtsTemplate) = c('X', 'Lat', 'Lon')
PtsTemplateGeo = st_as_sf(PtsTemplate, coords =c(3, 2), crs=4326)

intCDMVals = array(0, c(dim(PtsTemplate)[1]))

for(i in 1:length(strShpFilesInt)){
  strShpFile = strShpFilesInt[i]
  ShpData = read_sf(strShpFile)
  ShpDataTF = st_transform(ShpData, st_crs(PtsTemplateGeo))
  ShpIntersects = st_intersection(ShpDataTF, PtsTemplateGeo)
  
  intCDMVals[ShpIntersects$X] = i
}

PtsDroughtOut = cbind(PtsTemplate, intCDMVals)
PtsDroughtOut[,3] = PtsDroughtOut[,3] + 360

dir.create(strOutputDir)
write.csv(PtsDroughtOut, paste0(strOutputDir, 'CDM.csv'), row.names = FALSE)
