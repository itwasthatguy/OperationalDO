REM Batch script to automatically acquire and append GRIB2 format GEPSs forecasts. Makes use of wget to download from cmc website. After all files have been downloaded, calls Prepare24HForecastGribsAuto.R which appends the daily values to the ongoing CSV format forecast records.

@echo off
setlocal EnableDelayedExpansion

cd /d %~dp0

cd ..

set Dir=%cd%
set WgetDir=!Dir!/Processes/


set YYYYMMDD=%DATE:~0,4%%DATE:~5,2%%DATE:~8,2%
for /f %%a in ('wmic path win32_localtime get dayofweek /format:list ^| findstr "="') do (set %%a)
REM 0 = sunday, 6 = saturday


if %dayofweek%==4 (
set max=750
) else (
set max=30
)

for /l %%A in (6,24,%max%) Do (

set "fileName1=CMC_geps-raw_APCP_SFC_0_latlon0p5x0p5_!YYYYMMDD!00_P"


set "hour=000%%A"

set "fileName2=!fileName1!!hour:~-3!*"
!WgetDir!\wget.exe -r -p -np -P !Dir!\24HForecasts\Grib\ robots=off https://dd.weather.gc.ca/ensemble/geps/grib2/raw/00/!hour:~-3!/ -A !filename2!

)

for /l %%A in (6,3,%max%) Do (

set "fileName1=CMC_geps-raw_TMP_TGL_2m_latlon0p5x0p5_!YYYYMMDD!00_P"


set "hour=000%%A"

set "fileName2=!fileName1!!hour:~-3!*"
!WgetDir!\wget.exe -r -p -np -P !Dir!\24HForecasts\Grib\ robots=off https://dd.weather.gc.ca/ensemble/geps/grib2/raw/00/!hour:~-3!/ -A !filename2!

)

REM YYYY-MM-DD 		The dashes are neccessary
set DateHyphen=%DATE:~0,4%-%DATE:~5,2%-%DATE:~8,2%

REM R script to append daily forecast values into CSV files
"C:/Program Files/R/R-4.0.2/bin/x64/Rscript" !Dir!/Processes/DailyUpdate.R --args !DateHyphen! !Dir!

pause
