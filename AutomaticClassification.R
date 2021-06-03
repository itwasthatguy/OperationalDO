#Automatic version of PCA drought classification. This will run directly off of the main batch file rather than requiring a manual run.

#A classifier is trained with the historical data (2005-2015), and then makes 21 classifications (from 21 ensemble member forecasts) of the "forecast month". This represents the expected conditions by the end of the month.

#A crude pseudo-equation for the classification is as follows:  Predicted drought = Prior drought + monthly drought indices + month of the year + ecozone


library(keras)
library(parallel)
library(doParallel)
library(foreach)

Normalize = function(InVec, Max, Min){
  OutVec = (InVec - Min) / (Max - Min)
  #Won't affect the training run, but forecast values outside range will be set to the min or max 
  OutVec[which(OutVec > 1)] = 1
  OutVec[which(OutVec < 0)] = 0
  return(OutVec)
}

EnsembleMeanWrite = function(AllData, IndicatorsPath){    #AllData must be a 3d array - ensemble x point x indicator
  class(AllData) = "numeric"
  AllData[which(is.infinite(AllData))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
  AllData[which(is.na(AllData))] = -4
  EnsembleMeans = array(0, c(dim(AllData)[2:3]))
  for(i in 1:dim(AllData)[2]){
    EnsembleMeans[i,] = apply(AllData[,i,], 2, mean)
  }
  colnames(EnsembleMeans) = c('Lat', 'Lon', 'Year', 'Month', 'SPI1', 'SPI2', 'SPI3', 'SPI4', 'SPI5', 'SPI6', 'SPI7', 'SPI8', 'SPI9', 'SPI10', 'SPI11', 'SPI12'  , 'SPEI1', 'SPEI2', 'SPEI3', 'SPEI4', 'SPEI5', 'SPEI6', 'SPEI7', 'SPEI8', 'SPEI9','SPEI10', 'SPEI11', 'SPEI12',  'PDI', 'Drought', 'Drought-1', 'Drought-2', 'Drought-3', 'Drought-4', 'Drought-5', 'Drought-6', 'EcoZone')
  
  write.csv(EnsembleMeans, IndicatorsPath, quote=FALSE, row.names = FALSE)
  
}

######### Set the date and the directory accodingly!

ForecastDate = as.Date(commandArgs(trailingOnly = TRUE)[2])
MainDirectory = paste0(commandArgs(trailingOnly = TRUE)[3], '\\')

#########

IndexCount = (12*2) + 1 + 2 + 2 + 1 + 7  #Not just indices - 12 months of SPI, SPEI + 1 PDI value + Lat/Lon + Year/Month + Ecozone + current and past 6 months of drought. We only use the last month, but previous months could be included as a potential way of separating long and short term drought.


#These two groupings should be identical, but training groups could include extra ecozones. For example, we want to classify ecozone N. Ecozone N has very poor training data, so we want to train the classifier with ecozone N and M. We can later train another classifier only on ecozone M for the classification of ecozone M. This allows us to leverage the better training data of ecozone M without degrading the classification of ecozone M.
TrainGroups = list(c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15))
ClassifyGroups = list(c(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15))

ClassificationOutputDir = paste0(MainDirectory, "Outcomes\\Classifications\\")
PreviousOutputDir = paste0(MainDirectory, "Outcomes\\Prior\\")

TrainingDataFile = paste0(MainDirectory, 'Historical\\FullTrainingData.csv')
TrainingData = read.csv(TrainingDataFile)

SPIForecastDir = paste0(MainDirectory, 'Indices\\SPI\\', ForecastDate, '\\')
SPEIForecastDir = paste0(MainDirectory, 'Indices\\SPEI\\', ForecastDate, '\\')
PDIForecastDir = paste0(MainDirectory, 'Indices\\PDI\\', ForecastDate, '\\')
ForecastDroughtFile = paste0(MainDirectory, 'Indices\\CDM_Previous\\', ForecastDate, '\\CDM.csv')
EcoZonesFile = paste0(MainDirectory, '\\Misc\\PointLocationsEco.csv')
IndicatorsOutput = paste0(MainDirectory, 'Indices\\EnsembleMeans\\', ForecastDate, '.csv')

if(!dir.exists(paste0(MainDirectory, 'Indices\\EnsembleMeans\\'))) dir.create(paste0(MainDirectory, 'Indices\\EnsembleMeans\\'))

SPIForecastDir0 = paste0(SPIForecastDir, '0', '\\')
SPEIForecastDir0 = paste0(SPEIForecastDir, '0', '\\')
SPIFormatFiles = list.files(SPIForecastDir0)
SPEIFormatFiles = list.files(SPEIForecastDir0)

IntFiles = read.csv(paste0(MainDirectory, '\\Misc\\ForecastingPoints.csv'))
IntFiles= paste0(IntFiles[,1], '_', IntFiles[,2], '.csv')
IntFiles = intersect(IntFiles, SPEIFormatFiles)
IntFiles = intersect(IntFiles, SPIFormatFiles) 



PredictArray = array(0, c(21, length(IntFiles), IndexCount))    #The 21 is for the 21 ensemble members

clCount = detectCores() - 4

cl = makeCluster(clCount)			
registerDoParallel(cl)

Out = foreach(Par = 1:8) %dopar% {  #This 40 and the 40s below will need to be changed based on your core count.
  
  Lower = floor(((Par - 1) * (length(IntFiles)/8)) + 1)  #Calculate the set of files to be run - saves the thread from having to come back and find out
  Upper = floor(((Par) * (length(IntFiles)/8)))          #which file to run each time.
  
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

for(Par in 1:8){
  Block = Out[[Par]]
  
  Lower = floor(((Par - 1) * (length(IntFiles)/8)) + 1)
  Upper = floor(((Par) * (length(IntFiles)/8)))
  
  for(Mem in 1:21){
    Slice = Block[Mem,,]
    
    PredictArray[Mem,Lower:Upper,] = Slice
    
  }
}

DroughtData = read.csv(ForecastDroughtFile)
DroughtData = as.matrix(DroughtData)
colnames(DroughtData) = NULL
DroughtData = as.array(DroughtData[,1:ncol(DroughtData)])

for(DroughtLoc in 1:(dim(DroughtData)[1])){
  Loc = intersect(which(PredictArray[1,,1] == DroughtData[DroughtLoc,2]),which(PredictArray[1,,2] == DroughtData[DroughtLoc,3]))
  PredictArray[,Loc,31] = DroughtData[DroughtLoc, dim(DroughtData)[2]]
}


EcozonesData = read.csv(EcoZonesFile)

for(EcoPoint in 1:dim(EcozonesData)[1]){
  EcoZonePoint = EcozonesData[EcoPoint,]
  
  PredictLocs = intersect(which(PredictArray[1,,1] == EcoZonePoint[,1]), which(PredictArray[1,,2] == EcoZonePoint[,2]))
  PredictArray[,PredictLocs, 37] = EcozonesData[EcoPoint,3]
}


TrainMatFull = as.matrix(TrainingData)

#The training data has locations of "not classified" which need to be removed from the training set.

InvalidDrought = vector()
for(InvalidIter in 0:6){
  InvalidDrought = union(which(TrainMatFull[,(30 + InvalidIter)] == "6"), InvalidDrought)
}

TrainMatFull = TrainMatFull[-InvalidDrought,]

class(TrainMatFull) = 'numeric'
colnames(TrainMatFull) = c('Lat', 'Lon', 'Year', 'Month', 'SPI1', 'SPI2', 'SPI3', 'SPI4', 'SPI5', 'SPI6', 'SPI7', 'SPI8', 'SPI9', 'SPI10', 'SPI11', 'SPI12'  , 'SPEI1', 'SPEI2', 'SPEI3', 'SPEI4', 'SPEI5', 'SPEI6', 'SPEI7', 'SPEI8', 'SPEI9','SPEI10', 'SPEI11', 'SPEI12',  'PDI', 'Drought', 'Drought-1', 'Drought-2', 'Drought-3', 'Drought-4', 'Drought-5', 'Drought-6', 'EcoZone')


#Now we loop through groupings of ecozones
TrainMatFullbk = TrainMatFull

EnsembleMeanWrite(PredictArray, IndicatorsOutput)

TrainGroup = 1:15
ClassifyGroup = 1:15

TrainMatZone = TrainMatFull[which(TrainMatFull[,37] %in% TrainGroup),]

TrainMatChange = TrainMatZone[which(TrainMatZone[,30] != TrainMatZone[,31]),]
TrainMatChangeNot = TrainMatZone[which(TrainMatZone[,30] == TrainMatZone[,31]),]

SubSampleMat = TrainMatChange[which(TrainMatChange[,30] >= 4),]

Samp = sample(which(TrainMatChange[,30] == 4), size = min(c(500, length(which(TrainMatChange[,30] == 4)))))
SubSampleMat = rbind(SubSampleMat, TrainMatChangeNot[Samp,])
Samp = sample(which(TrainMatChange[,30] == 3), size = min(c(1600, length(which(TrainMatChange[,30] == 3)))))
SubSampleMat = rbind(SubSampleMat, TrainMatChange[Samp,])
Samp = sample(which(TrainMatChange[,30] == 2), size = min(c(3300, length(which(TrainMatChange[,30] == 2)))))
SubSampleMat = rbind(SubSampleMat, TrainMatChange[Samp,])
Samp = sample(which(TrainMatChange[,30] == 1), size = min(c(3500, length(which(TrainMatChange[,30] == 1)))))
SubSampleMat = rbind(SubSampleMat, TrainMatChange[Samp,])
Samp = sample(which(TrainMatChange[,30] == 0), size = min(c(3500, length(which(TrainMatChange[,30] == 0)))))
SubSampleMat = rbind(SubSampleMat, TrainMatChange[Samp,])


attr(TrainMatZone, 'dimnames') = NULL
attr(SubSampleMat, 'dimnames') = NULL

XTrain = as.array(SubSampleMat[,c(4:29,31)])
XTrain[which(is.infinite(XTrain))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
XTrain[which(is.na(XTrain))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
YTrain = as.array(SubSampleMat[,30])
class(YTrain) = 'integer'

length(which(YTrain == 4))

if(length(YTrain) == 0) next

XTrainSub = XTrain[,c(1,2:5,14:17,27)]
XTrainSub[which(XTrainSub[,10] == 0),10] = 1
XTrainSub[,10] = XTrainSub[,10] - 1

XTrainSubNorm = XTrainSub
NormMaxes = c()
NormMins = c()
for(i in 1:dim(XTrainSubNorm)[2]){
  NormMax = max(XTrainSubNorm[,i])
  NormMin = min(XTrainSubNorm[,i])
  
  NormMaxes = c(NormMaxes, NormMax)
  NormMins = c(NormMins, NormMin)
  
  XTrainSubNorm[,i] = Normalize(XTrainSubNorm[,i], NormMax, NormMin)
  
}

pca = prcomp(XTrainSubNorm, scale=TRUE)

YTrain[which(YTrain == 0)] = 1
YTrain = YTrain - 1

Input = pca$x[,1:8]

Random = sample(1:length(YTrain), length(YTrain), replace = FALSE)
Input = Input[Random,]
YTrain = YTrain[Random]


#########

model <- keras_model_sequential() %>%
  layer_dense(units = 60, activation = 'linear', input_shape = (dim(Input)[2])) %>%
  layer_dense(units = 20, activation = 'linear', input_shape = (dim(Input)[2])) %>%
  layer_dense(units = 6, activation = 'softmax')

# Compile model
model %>% compile(
  loss = 'sparse_categorical_crossentropy',
  optimizer = optimizer_adam(lr = 0.0001, beta_1 = 0.9, beta_2 = 0.999,
                             epsilon = NULL, decay = 0.0000000000000, amsgrad = FALSE, clipnorm = NULL,
                             clipvalue = NULL),
  metrics = c('accuracy')
)

# Train model
history = model %>% fit(
  Input, YTrain,
  batch_size = 512,#512
  epochs = 100,
  validation_split = 0.02
)

FullOut = c()
Counter = 1

for(Mem in 1:21){
  DataArray = PredictArray[Mem,,]
  #DataArray = DataArray[which(DataArray[,37] %in% ClassifyGroup),]
  class(DataArray) = 'numeric'
  
  XPredict = as.array(DataArray[,c(4:29,31)])
  XPredict[which(is.infinite(XPredict))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
  XPredict[which(is.na(XPredict))] = -4        #Assume all infs are neg infs in spi or spei caused by 0 precip months
  YPredict = as.array(DataArray[,30])
  class(YPredict) = 'integer'
  
  if(length(YPredict) == 0) next
  
  XPredictSub = XPredict[,c(1,2:5,14:17,27)]
  
  XPredictSub[which(XPredictSub[,10] == 0),10] = 1
  XPredictSub[,10] = XPredictSub[,10] - 1
  
  XPredictSubNorm = XPredictSub
  for(i in 1:dim(XPredictSubNorm)[2]){
    
    XPredictSubNorm[,i] = Normalize(XPredictSubNorm[,i], NormMaxes[i], NormMins[i])
    
  }
  
  Loaded = predict(pca, XPredictSubNorm)[,1:8]
  
  YPredict[which(YPredict == 0)] = 1
  YPredict = YPredict - 1
  
  Results = predict(model, Loaded)
  ResultsClass = apply(Results, 1, FUN = which.max) - 1
  
  #table(ResultsClass, YPredict)
  table(ResultsClass, XPredictSub[,10])
  
  #View(cbind(DataArray, ResultsClass))
  
  Output = cbind(DataArray[,1], DataArray[,2])
  Output = cbind(Output, ResultsClass)
  
  #View(XPredict[which(ResultsClass == 1),])
  
  if(!dir.exists(paste0(ClassificationOutputDir, ForecastDate, '\\'))) dir.create(paste0(ClassificationOutputDir, ForecastDate, '\\'))
  write.csv(Output, paste0(ClassificationOutputDir, ForecastDate, '\\', Mem, '.csv'), row.names=FALSE, quote=FALSE)
  
  Output = cbind(DataArray[,1], DataArray[,2])
  Output = cbind(Output, XPredictSub[,10])
  
  if(!dir.exists(paste0(PreviousOutputDir, ForecastDate, '\\'))) dir.create(paste0(PreviousOutputDir, ForecastDate, '\\'))
  write.csv(Output, paste0(PreviousOutputDir, ForecastDate, '\\', Mem, '.csv'), row.names=FALSE, quote=FALSE)
  
  if(Mem == 1) FullOut = cbind(FullOut, DataArray[,1], DataArray[,2])
  FullOut = cbind(FullOut, ResultsClass)
  
}

MedOut = array(0, c(3189,3))

for(i in 1:3189){
  MedOut[i,3] = median(FullOut[i, 3:23])
}

MedOut[,1:2] = FullOut[,1:2]