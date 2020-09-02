REM The main monthly forecast generator file. This will call all neccessary processes to create a set of SPI, SPEI, and PDI values across all 21 ensemble members for a desired month.

@echo off
setlocal EnableDelayedExpansion

REM This doesn't explicitly have to be the first thursday of the month. As long as StartDate represents a thursday with less than 32 days until the end of a month. For example, Jan 30, 2020 would work for a february 2020 forecast. On the other hand, there's no reason why Feb 27, 2020 also wouldn't work for a february 2020 forecast.

set /p StartDate="Enter first day (thursday) as YYYY-MM-DD: "
set /p EndDate="Enter last day of same month as YYYY-MM-DD: "

cd /d %~dp0

cd ..
cd ..

set MainDir=%cd%

if not exist "MonthlyForecasts/!StartDate!" mkdir "MonthlyForecasts/!StartDate!"
if not exist "MonthlyForecasts/!StartDate!CSV" mkdir "MonthlyForecasts/!StartDate!CSV"

echo !MainDir!

REM Bias corrections are fit on a per-forecast basis, so this step will require a lot of downloading and will take a while to run. Will generate ../Misc/BiasCorrections/YYYY-MM-DD/..

REM "C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !MainDir!/Processes/Hindcasting/FitBiasCorrectionMappings_3Week.R --args !StartDate! !EndDate! !MainDir!

REM Reads the already downloaded forecast gribs and converts them to CSV. These are not yet the long form forecasts, just the latest month. Generates ../MonthlyForecasts/YYYY-MM-DDCSV/..

REM "C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !MainDir!/Processes/Hindcasting/Hindcast_Prepare32DayForecastGribsAuto.R --args !StartDate! !EndDate! !MainDir!

REM Takes the CSV from previous and appends it to the ongoing timeseries of forecasts. Applies a transform to the newer data (the ongoing timeseries is pre-transformed). The transformation is pre-generated and can be applied on the fly, unlike the bias correction. Generates ../MonthlyForecasts/YYYY-MM-DDForecast/..

REM "C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !MainDir!/Processes/Hindcasting/Hindcast_CorrectAndAppendForecasts.R --args !StartDate! !EndDate! !MainDir!

REM Sums monthly precip and formats for reading into Richard's SPI program. Generates ../MonthlyForecasts/AccumulatedPrecip/YYYY-MM-DD/..

REM "C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !MainDir!/Processes/Hindcasting/Hindcast_SPI_Format_Ensembles.R --args !StartDate! !EndDate! !MainDir!

REM Richard's PDI program. Calculates PDI and P-PE on all ensembles. Generates ../Indices/PDI/YYYY-MM-DD/.. Including SPEI Input Sub-dir

REM !MainDir!/Processes/Hindcasting/HindcastPDI.exe !StartDate! !EndDate!

REM Richard's SPI program. Calculates SPI on all ensembles. Generates ../Indices/SPI/YYYY-MM-DD/..

REM !MainDir!/Processes/Hindcasting/HindcastSPI.exe !StartDate! !EndDate!

REM Another R script, calling the 'spei' package to calculated SPEI based on the SPEI Input from Ricahrd's PDI program. Generates ../Indices/SPEI/YYYY-MM-DD/..

REM "C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !MainDir!/Processes/Hindcasting/HindcastSPEI.R --args !StartDate! !EndDate! !MainDir!

pause

