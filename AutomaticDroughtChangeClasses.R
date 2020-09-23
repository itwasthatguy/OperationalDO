
Date = as.Date(commandArgs(trailingOnly = TRUE)[2])
MainDir = paste0(commandArgs(trailingOnly = TRUE)[3], '\\')

OutputFile = paste0(MainDir, 'Outcomes\\ResultsClasses', Date, '.csv')

ClassMainDir = paste0(MainDir, 'Outcomes\\Classifications\\')
PrevMainDir = paste0(MainDir, 'Outcomes\\Prior\\')
ClassDir = paste0(ClassMainDir, Date, '\\')
PrevDir = paste0(PrevMainDir, Date, '\\')

Files = list.files(ClassDir, full.names=TRUE)

Template = array(0, c(0, 3))

LocsTotal = 0
File = paste0(ClassDir, '1.csv')
Data = read.csv(File)
Template = rbind(Template, Data)
Locs = dim(Data)[1]
LocsTotal = LocsTotal + Locs


DroughtArray = array(0, c(dim(Template)[1], 23))
PreviousArray = array(0, c(dim(Template)[1], 23))

DroughtArray[,1] = Template[,1]
DroughtArray[,2] = Template[,2]

PreviousArray[,1] = Template[,1]
PreviousArray[,2] = Template[,2]

RowCount = 1
for(Member in 1:21){
  File = paste0(ClassDir, Member, '.csv')
  Data = read.csv(File)
  DroughtArray[,2 + Member] = Data[,3]
}

Files = list.files(PrevDir, full.names=TRUE)

for(Member in 1:21){
  File = paste0(PrevDir, Member, '.csv')
  Data = read.csv(File)
  PreviousArray[,2 + Member] = Data[,3]
}

PreviousStates = PreviousArray[,3]

ProbabilityArray = array(0, c(dim(DroughtArray)[1], 6))

for(Drought in 0:5){
  for(Loc in 1:dim(DroughtArray)[1]){
    ProbabilityArray[Loc, Drought+1] = (length(which(DroughtArray[Loc,] == Drought))/21)
  }
}

ChanceDrought = apply(ProbabilityArray[,2:6], 1, sum)
ChanceNoDrought = ProbabilityArray[,1]

#Divide into 4 options of binary drought or not drought, staying the same or changing either way
#This is what index locations contain each option

DroughtToDrought = intersect(which(ChanceDrought > 0.5), which(PreviousStates >= 1))
ClearToClear = intersect(which(ChanceDrought < 0.5), which(PreviousStates < 1))
ClearToDrought = intersect(which(ChanceDrought > 0.5), which(PreviousStates < 1))
DroughtToClear = intersect(which(ChanceDrought < 0.5), which(PreviousStates >= 1))

#Use ChanceDrought and ChanceNoDrought as the confidence values for ClearToDrought and DroughtToClear
#For DroughtToDrought, we want to sum probabilities that are lower, equal, and higher than PreviousStates


Confidences = vector(mode='numeric', length=length(PreviousStates))

#Confidence for all but drought to drought can just be the chance that it ends up in that category.
Confidences[ClearToDrought] = ChanceDrought[ClearToDrought]
Confidences[DroughtToClear] = ChanceNoDrought[DroughtToClear]
Confidences[ClearToClear] = ChanceNoDrought[ClearToClear]

OutputClasses = array(0, c(length=length(PreviousStates), 4))
OutputClasses[,1:2] = DroughtArray[,1:2]

for(ChangeDrought in DroughtToDrought){
  Droughts = DroughtArray[ChangeDrought,3:23]
  Lower = which((1:6) < PreviousStates[ChangeDrought])
  Same = which((1:6) == PreviousStates[ChangeDrought])
  Higher = which((1:6) > PreviousStates[ChangeDrought])
  
  if(length(Lower) > 0){
    DownChange = length(which(Droughts %in% Lower))/21
  } else {
    DownChange = 0
  }
  
  DownChange = DownChange + ChanceNoDrought[ChangeDrought]
  
  if(length(Higher) > 0){
    UpChance = length(which(Droughts %in% Higher))/21
  } else {
    UpChance = 0
  }
  
  SameChance = length(which(Droughts %in% Same))/21
  Direction = which.max(c(DownChange, SameChance, UpChance))    #1 for lower, 2 for same, 3 for higher
  Conf = max(c(DownChange, SameChance, UpChance))               #And confidence of it
  
  OutputClasses[ChangeDrought,3] = Direction + 2
  OutputClasses[ChangeDrought,4] = Conf
  
}

OutputClasses[ClearToDrought,3] = 2
OutputClasses[ClearToDrought,4] = Confidences[ClearToDrought]

OutputClasses[DroughtToClear,3] = 1
OutputClasses[DroughtToClear,4] = Confidences[DroughtToClear]

OutputClasses[ClearToClear,3] = 0
OutputClasses[ClearToClear,4] = Confidences[ClearToClear]


ConfThresh = c()
for(Conf in seq(0.5, 1, 0.05)){
  ConditionalClass = c()
  for(Loc in 1:length(PreviousStates)){
    if(OutputClasses[Loc,4] >= Conf){
      ConditionalClass = c(ConditionalClass, OutputClasses[Loc,3])
    } else if(OutputClasses[Loc,3] %in% c(1,3,4,5)){
      ConditionalClass = c(ConditionalClass, 4)
    } else {
      ConditionalClass = c(ConditionalClass, 6)
    }
    
  }
  
  ConfThresh = cbind(ConfThresh, ConditionalClass)
  
}

OutputClassesThresh = cbind(OutputClasses, ConfThresh)

colnames(OutputClassesThresh) = c('Lat', 'Lon', 'Class', 'Confidence', '50%', '55%', '60%', '65%', '70%', '75%', '80%', '85%', '90%', '95%', '100%')

write.csv(OutputClassesThresh, OutputFile, quote = FALSE, row.names = FALSE)
