#This script exists to construct the neural network model which will then classify future drought states. This is done through training on a historical data set, and then classifying corresponding forecasted data. The majority of this data is pre-sorted and organized in such a way that the file paths do not need to be altered. Currently, the dates (just below) need to be adjusted based on which month is being forecasted. This is just because the data is named based on the forecast date and month. Also currently, drought conditions are not automatically generated for forecasted dates. This will need to be adjusted. Parallel processing is also used (to read files). This is currently hardcoded as 40 threads, so if your cpu core count is lower, you should adjust this. Also be sure to adjust the "Upper" and "Lower" variables - these subset the files into groupings for more efficient processing.

#This process takes a while to read in all the data, and then to train, but can be run in an afternoon.

#Locations are subset into "EcoGroups" or groupings of ecozones. These are chosen based on geographical position, and some testing to determine effective combinations. Certain ecozones have been divided by province in the case that they cover a large area. For example, the boreal ecozone covers basically half of Canada, so it makes little sense to use it in combination with any other ecozone, or even alone.

library(keras)
library(parallel)
library(doParallel)
library(foreach)

DummyDate = as.Date("2020-01-02")   #Set this as your forecast date

PredictionMonth = as.Date("2020-01-01")  #Input the date as the first of the month you're interested in predicting. We assume that it will represent the last of the month.

TrainingMonths = 144
IndexCount = (12*2) + 1 + 2 + 2 + 1 + 7  #Not just indices - 12 months of SPI, SPEI + 1 PDI value + Lat/Lon + Year/Month + Ecozone + current and past 6 months of drought

#In contrast, we assume the training period to simply be January 2004 to Dec 2015

#Input file directories here - We need SPI, SPEI, and PDI for both training (Gridded) and predicting (Forecast) data sets

SPIForecastDir = paste0('E:\\AutomaticDO\\Indices\\SPI\\', DummyDate, '\\')
SPEIForecastDir = paste0('E:\\AutomaticDO\\Indices\\SPEI\\', DummyDate, '\\')
PDIForecastDir = paste0('E:\\AutomaticDO\\Indices\\PDI\\', DummyDate, '\\')

SPIGridDir = 'E:\\GriddedWeather\\SPI\\'
SPEIGridDir = 'E:\\GriddedWeather\\SPEI\\'
PDIGridDir = 'E:\\GriddedWeather\\PDI Output\\'

#Other directories

TrainingDroughtFile = 'E:\\Droughts\\DroughtAllYears\\TrainingDroughtIntersections.csv'  #Contains all drought information for all location for all month   
TestingDroughtFile = 'E:\\AutomaticDO\\ValidationDrought\\NewForecastDroughts.csv'

EcoZonesPath = 'E:\\Droughts\\EcoZones\\HalfGridClimateZones_BorealSplit.csv'

PointFile = 'E:\\Droughts\\HalfGridPoints_Project.shp'

MainOutDir = 'E:\\AutomaticDO\\ValidationDrought\\PredsANN\\'

EcoGroups = list(c(12,13,14,4), c(9, 10, 16, 18), c(15, 17, 19, 8), c(3, 5, 1, 20, 7, 21))

#Since the locations are not perfectly 1:1, we need to find the intersection of all files. For the forecasts, member 0 will be used as each member will be identical in locations.

#Also due to PDI containing an SPEI subfolder, it will be skipped in intersection finding. This is alright, as the locations of SPEI will be identical to PDI.

SPIForecastDir0 = paste0(SPIForecastDir, '0', '\\')
SPEIForecastDir0 = paste0(SPEIForecastDir, '0', '\\')

SPIG = list.files(SPIGridDir)
SPIF = list.files(SPIForecastDir0)

SPEIG = list.files(SPEIGridDir)
SPEIF = list.files(SPEIForecastDir0)

SPI_Int = intersect(SPIG, SPIF)
SPEI_Int = intersect(SPEIG, SPEIF)
IntFiles = intersect(SPI_Int, SPEI_Int)     #Every index of both forecast types will have these locations

TrainArray = array(0, c(TrainingMonths * length(IntFiles), IndexCount))

PredictArray = array(0, c(21, length(IntFiles), IndexCount))    #The 21 is for the 21 ensemble members

cl = makeCluster(40)    #Arbitrary use of 40 threads based off of my CPU's specs. Check yours and pick a number slightly below your core count.
registerDoParallel(cl)

Out = foreach(Par = 1:40) %dopar% {
  
  Lower = floor(((Par - 1) * (length(IntFiles)/40)) + 1)  #Calculate the set of files to be run - saves the thread from having to come back and find out
  Upper = floor(((Par) * (length(IntFiles)/40)))          #which file to run each time.
  
  PredictArraySub = array(0, c(21, length(Lower:Upper), IndexCount))  #A small set of PredictArray with index 2 equal to the file count in this thread
  
  for(FileNum in Lower:Upper){
    File = IntFiles[FileNum]
    
    LatLon = unlist(strsplit(File,'.csv'))[1]   #Take the file's geographical location from the file name
    Lat = unlist(strsplit(LatLon,'_'))[1]
    Lon = unlist(strsplit(LatLon,'_'))[2]
    
    for(Member in 0:20){  #Go through each member and read in, based on the first index, the member specific data
      
      SPIForecastDirMem = paste0(SPIForecastDir, Member, '\\')
      SPEIForecastDirMem = paste0(SPEIForecastDir, Member, '\\')
      PDIForecastDirMem = paste0(PDIForecastDir, Member, '\\')
      
      SPIData = read.table(paste0(SPIForecastDirMem, File), header=FALSE)  #SPI has no header. We want the first 14 cols (Year, month, SPI 1-12)
      SPEIData = read.csv(paste0(SPEIForecastDirMem, File))    #SPEI and PDI have headers. We want columns 3-14 for SPEI 1-12 (1 and 2 are year, month but we already have that from SPI)
      PDIData = read.csv(paste0(PDIForecastDirMem, LatLon, '_PDI.csv'))    #We only want column 4 for the PDI value.
      
      SPIData = as.matrix(SPIData)[dim(SPIData)[1],1:14]          #Only take the last month, as represented by the first index being equal to the first
      SPEIData = as.matrix(SPEIData)[dim(SPEIData)[1],3:14]       #index length. The second index represents, for example, the one to twelve month SPI.
      PDIData = as.matrix(PDIData)[dim(PDIData)[1],4]
      
      PredictArraySub[Member+1,FileNum - Lower + 1,1] = Lat
      PredictArraySub[Member+1,FileNum - Lower + 1,2] = Lon
      PredictArraySub[Member+1,FileNum - Lower + 1,3:16] = SPIData
      PredictArraySub[Member+1,FileNum - Lower + 1,17:28] = SPEIData
      PredictArraySub[Member+1,FileNum - Lower + 1,29] = PDIData
      
    }
    
  }
  
  return(PredictArraySub) #Foreach is a function - return the subset predict array
}

stopCluster(cl)

#The previous block returned a list of subset predict arrays - arranged in a way that if bound on index 2, they would form a full block of predicted drought indices. The following section steps through each list item and merges them together into PredictArray, which is a singular array.

for(Par in 1:40){
  Block = Out[[Par]]
  
  Lower = floor(((Par - 1) * (length(IntFiles)/40)) + 1)
  Upper = floor(((Par) * (length(IntFiles)/40)))
  
  for(Mem in 1:21){
    Slice = Block[Mem,,]
    
    PredictArray[Mem,Lower:Upper,] = Slice
    
  }
}

#Here we need to read in the training data from the historical sets. There are no ensembles, so parallel reading isn't as important to us.


for(FileNum in 1:length(IntFiles)){
  File = IntFiles[FileNum]
  
  LatLon = unlist(strsplit(File,'.csv'))[1]   #Take the file's geographical location from the file name
  Lat = unlist(strsplit(LatLon,'_'))[1]
  Lon = unlist(strsplit(LatLon,'_'))[2]
  
  SPIData = read.table(paste0(SPIGridDir, File), header=FALSE)  #SPI has no header. We want the first 14 cols (Year, month, SPI 1-12)
  SPEIData = read.csv(paste0(SPEIGridDir, File))    #SPEI and PDI have headers. We want columns 3-14 for SPEI 1-12 (1 and 2 are year, month but we already have that from SPI)
  PDIData = read.csv(paste0(PDIGridDir, LatLon, '_PDI.csv'))    #We only want column 4 for the PDI value.
  
  DataLength = dim(SPIData)[1]    #Since we only want Jan 2003 to Dec 2015, we can find the length of the data ending in Dec 2015 and subtract that length to find the start point
  IndexStart = ((DataLength - TrainingMonths)+1)
  
  SPIData = as.matrix(SPIData)[IndexStart:DataLength,1:14]
  SPEIData = as.matrix(SPEIData)[IndexStart:DataLength,3:14]
  PDIData = as.matrix(PDIData)[IndexStart:DataLength,4]
  
  TrainArray[(((FileNum-1) * TrainingMonths) + 1): ((FileNum) * TrainingMonths), 1] = Lat
  TrainArray[(((FileNum-1) * TrainingMonths) + 1): ((FileNum) * TrainingMonths), 2] = Lon
  TrainArray[(((FileNum-1) * TrainingMonths) + 1): ((FileNum) * TrainingMonths), 3:16] = SPIData
  TrainArray[(((FileNum-1) * TrainingMonths) + 1): ((FileNum) * TrainingMonths), 17:28] = SPEIData
  TrainArray[(((FileNum-1) * TrainingMonths) + 1): ((FileNum) * TrainingMonths), 29] = PDIData
  
}

#For training data, we have a pre-set drought file. This is a CSV of grid-based drought states. Index 1 corresponds to location, and index 2 contains the time (Monthly from Jan 2004 to Dec 2015). Note that index 2 locations 1 and 2 contain the Lat and Lon.

DroughtData = read.csv(TrainingDroughtFile)
DroughtData = as.matrix(DroughtData)
colnames(DroughtData) = NULL
DroughtData = as.array(DroughtData[,2:ncol(DroughtData)])
#DroughtData[which(DroughtData == 6)] = 0

for(DroughtLoc in 1:(dim(DroughtData)[1])){
  Loc = intersect(which(TrainArray[,1] == DroughtData[DroughtLoc,1]),which(TrainArray[,2] == DroughtData[DroughtLoc,2]))
  TrainArray[Loc,30] = DroughtData[DroughtLoc, 3:dim(DroughtData)[2]]
  
  #This block reads the prior drought states to give prior drought covariates. Currently set to up to 6 months previously, but we only actually use 1 month.
  
  for(Step in 1:6){
    NewLoc = Loc[(Step + 1):length(Loc)]
    TrainArray[NewLoc,(30 + Step)] = TrainArray[(Loc[1]):Loc[(length(Loc)-Step)],30]
  }
  
}

#This is a similar procedure but should be adjusted to be more automatic. Currently it takes the last column from a drought file made up of all forecast months.

DroughtData = read.csv(TestingDroughtFile)
DroughtData = as.matrix(DroughtData)
colnames(DroughtData) = NULL
DroughtData = as.array(DroughtData[,1:ncol(DroughtData)])

for(DroughtLoc in 1:(dim(DroughtData)[1])){
  Loc = intersect(which(PredictArray[1,,1] == DroughtData[DroughtLoc,2]),which(PredictArray[1,,2] == DroughtData[DroughtLoc,3]))
  PredictArray[,Loc,30] = DroughtData[DroughtLoc, dim(DroughtData)[2]]
  PredictArray[,Loc,31] = DroughtData[DroughtLoc, dim(DroughtData)[2] - 1]
}

#Assign the ecozones to each point. There is a file containing lat, lon, and ecozone intersection. This just finds the points and assigns the values. This is carried out for the training and predicted data.

EcozonesData = read.csv(EcoZonesPath)

for(EcoPoint in 1:dim(EcozonesData)[1]){
  EcoZonePoint = EcozonesData[EcoPoint,]
  TrainLocs = intersect(which(TrainArray[,1] == EcoZonePoint[,2]), which(TrainArray[,2] == EcoZonePoint[,3]))
  TrainArray[TrainLocs, 37] = EcozonesData[EcoPoint,4]
  
  PredictLocs = intersect(which(PredictArray[1,,1] == EcoZonePoint[,2]), which(PredictArray[1,,2] == EcoZonePoint[,3]))
  PredictArray[,PredictLocs, 37] = EcozonesData[EcoPoint,4]
}



# Locs = paste0(TrainArray[,1], TrainArray[,2])
# EcoLocs = paste0(EcozonesData[,2], EcozonesData[,3])
# 
# for(EcoPoint in which(intersect(EcoLocs, Locs) %in% Locs)){
#   Timeslots = (((EcoPoint -1) * 144) + 1):(EcoPoint * 144)
#   TrainArray[Timeslots, 37] = EcozonesData[EcoPoint,4]
#   
#   PredictArray[,EcoPoint,37] = EcozonesData[EcoPoint,4]
# }

TrainMat = as.matrix(TrainArray)

#The training data has locations of "not classified" which need to be removed from the training set.

InvalidDrought = vector()
for(InvalidIter in 0:6){
  InvalidDrought = union(which(TrainMat[,(30 + InvalidIter)] == "6"), InvalidDrought)
}

TrainMat = TrainMat[-InvalidDrought,]

#Create a full backup of TrainMat

TrainMatFull = TrainMat

#Cycle through each group of ecozones and train the network to classify the drought state. Covariates used (currently) are Latitude, Longiitude, Month, SPI 1-12, SPEI 1-12, PDI, Previous Drought (1 month), and Ecozone.

#Subsampling is used to better balance the drought classes. This is based on the number of D2s. D1s, 0s, and no droughts are scaled based off of this number. This allows the distribution of results to be closer to even. Due to the nature of the loss function being solved, rare classes are ignored somewhat, as it's statistically more useful to simply favour common classes. By balancing the classes, this problem is lessened. It would be good to investigate better ways to handle this.

EcoCount = 0
for(EcoGroup in EcoGroups){
  EcoCount = EcoCount + 1
  
  TrainMat = TrainMatFull[which(TrainMatFull[,37] %in% EcoGroup),]
  
  class(TrainMat) <- "numeric"
  
  colnames(TrainMat) = c('Lat', 'Lon', 'Year', 'Month', 'SPI1', 'SPI2', 'SPI3', 'SPI4', 'SPI5', 'SPI6', 'SPI7', 'SPI8', 'SPI9', 'SPI10', 'SPI11', 'SPI12'  , 'SPEI1', 'SPEI2', 'SPEI3', 'SPEI4', 'SPEI5', 'SPEI6', 'SPEI7', 'SPEI8', 'SPEI9','SPEI10', 'SPEI11', 'SPEI12',  'PDI', 'Drought', 'Drought-1', 'Drought-2', 'Drought-3', 'Drought-4', 'Drought-5', 'Drought-6', 'EcoZone')
  
  Samp0 = sample(which(TrainMat[,30] == 0), size = length(which(TrainMat[,30] == 3))*6)
  Samp1 = sample(which(TrainMat[,30] == 1), size = length(which(TrainMat[,30] == 3))*4)
  Samp2 = sample(which(TrainMat[,30] == 2), size = length(which(TrainMat[,30] == 3))*2)
  SubSampleMat = rbind(TrainMat[which(TrainMat[,30] > 2),], TrainMat[Samp2,])
  SubSampleMat = rbind(SubSampleMat, TrainMat[Samp1,])
  SubSampleMat = rbind(SubSampleMat, TrainMat[Samp0,])
  
  
  XTrain = as.array(SubSampleMat[,c(4:29,31,37)])
  XTrain[which(is.infinite(XTrain))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
  YTrain = as.array(SubSampleMat[,30])
  class(YTrain) = 'integer'
  
  model <- keras_model_sequential() %>%
    layer_dense(units = 40, activation = 'relu', input_shape = (28)) %>% 
    layer_dropout(rate=0.2) %>%
    layer_dense(units = 30, activation = 'relu') %>%
    #layer_dropout(rate=0.2) %>%
    layer_dense(units = 6, activation = 'softmax')
  
  #, kernel_regularizer = regularizer_l1_l2(l1 = 0.001, l2 = 0.01)
  
  # Compile model
  model %>% compile(
    loss = 'sparse_categorical_crossentropy',
    optimizer = optimizer_adam(lr = 0.0001, beta_1 = 0.9, beta_2 = 0.999,
                               epsilon = NULL, decay = 0.00000001, amsgrad = FALSE, clipnorm = NULL,
                               clipvalue = NULL),
    metrics = c('accuracy')
    #metrics = c('mean_absolute_error')
  )
  
  # Train model
  history = model %>% fit(
    XTrain, YTrain,
    batch_size = 256,
    epochs = 500,
    validation_split = 0.2
  )
  
  
  for(Mem in 1:21){
    DataArray = PredictArray[Mem,,]
    DataArray = DataArray[which(DataArray[,37] %in% EcoGroup),]
    class(DataArray) = 'numeric'
    
    XPredict = as.array(DataArray[,c(4:29,31,37)])
    XPredict[which(is.infinite(XPredict))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
    YPredict = as.array(DataArray[,30])
    class(YPredict) = 'integer'
    
    
    Results = predict(model, XPredict)
    ResultsClass = apply(Results, 1, FUN = which.max) - 1
    
    Output = cbind(DataArray[,1], DataArray[,2])
    Output = cbind(Output, ResultsClass)
    
    write.csv(Output, paste0('E:\\EnsembleMockup\\Results\\', Mem, '_', EcoCount, '.csv'), row.names=FALSE, quote=FALSE)
    
    Output = cbind(DataArray[,1], DataArray[,2])
    Output = cbind(Output, DataArray[,31])
    
    write.csv(Output, paste0('E:\\EnsembleMockup\\Previous\\', Mem, '_', EcoCount, '.csv'), row.names=FALSE, quote=FALSE)
    
  }
}