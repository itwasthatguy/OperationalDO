#Reshape the gridded data

#The gridded data is initially released at roughly 10km resolution. Each file is given as a .asc, compatible with ArcGIS and the like, and represents the full spatial extent of Canada, but only one day. Each value is essentially a pixel on a map.

#The purpose of this script is to "stack" days and then "slice" by location. We want to go from one file for one day to one file for all days, but one location. Additionally, we need to aggregate to 0.5 degree resolution to match the spatial resolution of the GEPS forecast.

#This script runs year by year, so the output files will need to be combined by year. For example, 44_280_1950 to 44_280_2015 will need to be combined.

MainDir = "E:\\1950_2015_Grid\\NewGridUnzipPrecip\\"
OutDir = "E:\\1950_2015_Grid\\NewGridTest\\Precip_Unstacked_Half\\"

Years = c(1950:2015)

#I counted the number of values in a file and got this.
Num = (544686 - 6)

FileArray = array(0, c(366, Num))
AssignmentArray = array(0, c(Num,2))    #Lat*2, Lon*2
OutDataArray = array(0, c(360, 720, 366, 2))	#For now 365 days for the test year... Eventually 25something thousand. Last index is value[1] and num to avg by[2]

TestingArray1 = array(0, c(Num))		#Because match doesn't work
TestingArray2 = array(0, c(Num))

#First, we want to figure out *where* the lat/lon thresholds are

#Basically I manually found out the bounds and resolution of this data. If you have different data, or a different version, you will need to adjust these values yourself. It's pretty simple though. This is the lowest Lon value, the highest Lat value, and the distance between each value. This does assume that the X and Y resolution are identical though. If not, it should be simple enough to make that adjustment.

#By good fortune, I've found that the distance between points is 1/12th of a degree. It's only approximately 10km.
Step = (1/12)
LonStart = 219				#Columns
LatStart = 83.5				#Rows

Counter = 1
for(i in 1:510){				#Rows - Lat
	for(j in 1:1068){			#Columns - Lon

		Lon =  LonStart + ((j-1) * Step)	#The top left of each pixel, essentially
		Lat = LatStart - ((i-1) * Step)

		Lon = floor((Lon + (1/24))* 2 + 0.5)/2	#Find the midpoint of the pixel, but round it.
		Lat = floor((Lat - (1/24))*2 + 0.5)/2

		AssignmentArray[Counter,] = c(Lat*2,Lon*2)
		TestingArray1[Counter] = Lat
		TestingArray2[Counter] = Lon
		Counter = Counter + 1
	}
}

for(Year in Years){

	OutDataArray = array(0, c(360, 720, 366, 2))	#Reset arrays
	FileArray = array(0, c(366, Num))

	YearString = paste(MainDir, Year, "\\", sep="")

	for(Day in 1:59){
		FileName = paste(YearString, "pcp", Year, "_", Day, ".asc", sep="")
		Read =read.csv(FileName, stringsAsFactors=FALSE, header=FALSE)
		FileArray[Day,] = as.numeric(as.character(Read[7:544686,1]))
	}

	if(file.exists(paste(YearString, "pcp", Year, "_60", ".asc", sep=""))){         #Day 60 is skipped if there is no leap day!
		FileName = paste(YearString, "pcp", Year, "_60.asc", sep="")
		Read =read.csv(FileName, stringsAsFactors=FALSE, header=FALSE)
		FileArray[60,] = as.numeric(as.character(Read[7:544686,1]))
	}

	for(Day in 61:366){
		FileName = paste(YearString, "pcp", Year, "_", Day, ".asc", sep="")
		Read =read.csv(FileName, stringsAsFactors=FALSE, header=FALSE)
		FileArray[Day,] = as.numeric(as.character(Read[7:544686,1]))
	}
		
	#All days of the year are assigned, and prior to that areas have been assigned
	#Now we slice and dice

	#Run through each value location, adding it to its respective OutDataArray location based off of AssignmentArray. Also add one to the "number of points" feeding into it (index 2)

	for(i in 1:Num){
		#if(!is.na(FileArray[Day,i]) && FileArray[Day,i] >= 0){                 #SWITCH THESE OUT for temp or precip - less than 0 is unreasonable for precip, less than -250 is unreasonable for temperature.
		if(!is.na(FileArray[Day,i]) && FileArray[Day,i] >= -250){
			for(Day in 1:366){
				OutDataArray[(AssignmentArray[i,1]), (AssignmentArray[i,2]),Day ,1] = OutDataArray[(AssignmentArray[i,1]), (AssignmentArray[i,2]),Day ,1] + FileArray[Day,i]
				OutDataArray[(AssignmentArray[i,1]), (AssignmentArray[i,2]),Day ,2] = OutDataArray[(AssignmentArray[i,1]), (AssignmentArray[i,2]),Day ,2] + 1
			}
		}
	}

	#Now just divide the summed value by the number of points feeding into it to get the average

	for(Lat in 1:360){
		for(Lon in 1:720){
			for(Day in 1:366){
				OutDataArray[Lat, Lon, Day,1] = OutDataArray[Lat, Lon, Day,1] / OutDataArray[Lat, Lon, Day,2]
			}
		}
	}

	#Save a file at each year - safer this way than having to redo everything if it breaks halfway through

	for(Lat in 1:360){
		for(Lon in 1:720){
			if(OutDataArray[Lat, Lon, 1,2] > 0){	#An easy way to see if any points have fed into this location - check the first day
				OutFile = paste(OutDir, Lat/2, "_", Lon/2, "_", Year, ".csv", sep="")
				write.table(OutDataArray[Lat,Lon,,1], file=OutFile, quote=FALSE, col.names=FALSE, row.names=FALSE, sep=",")

			}
		}
	}


}

	