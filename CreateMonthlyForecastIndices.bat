REM The main monthly forecast generator file. This will call all neccessary processes to create a set of SPI, SPEI, and PDI values across all 21 ensemble members for a desired month.

@echo off
setlocal EnableDelayedExpansion

REM This doesn't explicitly have to be the first thursday of the month. As long as StartDate represents a thursday with less than 32 days until the end of a month. For example, Jan 30, 2020 would work for a february 2020 forecast. On the other hand, there's no reason why Feb 27, 2020 also wouldn't work for a february 2020 forecast.

set /p StartDate="Enter first day (thursday) as YYYY-MM-DD: "
set /p EndDate="Enter last day of same month as YYYY-MM-DD: "

cd /d %~dp0

cd ..

set MainDir=%cd%

if not exist "MonthlyForecasts/!StartDate!" mkdir "MonthlyForecasts/!StartDate!"
if not exist "MonthlyForecasts/!StartDate!CSV" mkdir "MonthlyForecasts/!StartDate!CSV"

REM Bias corrections are fit on a per-forecast basis, so this step will require a lot of downloading and will take a while to run. Will generate ../Misc/BiasCorrections/YYYY-MM-DD/..

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/FitBiasCorrectionMappings.R --args !StartDate! !EndDate! !MainDir!

REM Reads the already downloaded forecast gribs and converts them to CSV. These are not yet the long form forecasts, just the latest month. Generates ../MonthlyForecasts/YYYY-MM-DDCSV/..

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/Prepare32DayForecastGribsAuto.R --args !StartDate! !EndDate! !MainDir!

REM Takes the CSV from previous and appends it to the ongoing timeseries of forecasts. Applies a transform to the newer data (the ongoing timeseries is pre-transformed). The transformation is pre-generated and can be applied on the fly, unlike the bias correction. Generates ../MonthlyForecasts/YYYY-MM-DDForecast/..

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/CorrectAndAppendForecasts_Month_NoRsquared.R --args !StartDate! !EndDate! !MainDir!

REM Sums monthly precip and formats for reading into Richard's SPI program. Generates ../MonthlyForecasts/AccumulatedPrecip/YYYY-MM-DD/..

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/SPI_Format_Ensembles.R --args !StartDate! !EndDate! !MainDir!

REM Richard's PDI program. Calculates PDI and P-PE on all ensembles. Generates ../Indices/PDI/YYYY-MM-DD/.. Including SPEI Input Sub-dir

!MainDir!/Processes/EnsembleForecastPDI.exe !StartDate! !EndDate!

REM Richard's SPI program. Calculates SPI on all ensembles. Generates ../Indices/SPI/YYYY-MM-DD/..

!MainDir!/Processes/EnsembleForecastSPI.exe !StartDate! !EndDate!

REM Another R script, calling the 'spei' package to calculated SPEI based on the SPEI Input from Ricahrd's PDI program. Generates ../Indices/SPEI/YYYY-MM-DD/..

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/AutoSPEIEnsembles.R --args !StartDate! !EndDate! !MainDir!

REM This process will automatically download the previous month's CDM. It then reads the CDM shape file and generates a CSV containing the CDM values at each grid location.

"C:/Program Files/R/R-3.6.1/bin/x64/Rscript" !MainDir!/Processes/ExtractCDM.R --args !StartDate! !EndDate! !MainDir!

