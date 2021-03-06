#Specify path to AutomaticDO folder. Generates directory structure if you are unable to locate AutomaticDO.rar.

#Credit to Diksha Goswami for making this.

Path = 'E:\\'

dir.create(paste0(Path,"AutomaticDO"))
dir.create(paste0(Path,"AutomaticDO\\24HForcasts"))
dir.create(paste0(Path,"AutomaticDO\\24HForcasts\\grib"))
dir.create(paste0(Path,"AutomaticDO\\24HForcasts\\CSV"))
dir.create(paste0(Path,"AutomaticDO\\CDM"))
dir.create(paste0(Path,"AutomaticDO\\Historical"))
dir.create(paste0(Path,"AutomaticDO\\Historical\\PDI"))
dir.create(paste0(Path,"AutomaticDO\\Historical\\PDI\\SPEI Input"))
dir.create(paste0(Path,"AutomaticDO\\Historical\\SPEI"))
dir.create(paste0(Path,"AutomaticDO\\Historical\\SPI"))
dir.create(paste0(Path,"AutomaticDO\\Indices"))
dir.create(paste0(Path,"AutomaticDO\\Indices\\CDM_Previous"))
dir.create(paste0(Path,"AutomaticDO\\Indices\\PDI"))
dir.create(paste0(Path,"AutomaticDO\\Indices\\SPEI"))
dir.create(paste0(Path,"AutomaticDO\\Indices\\SPI"))
dir.create(paste0(Path,"AutomaticDO\\Misc"))
dir.create(paste0(Path,"AutomaticDO\\Misc\\BiasCorrections"))
dir.create(paste0(Path,"AutomaticDO\\Misc\\PDICal"))
dir.create(paste0(Path,"AutomaticDO\\Misc\\SPEICal"))
dir.create(paste0(Path,"AutomaticDO\\Misc\\SPICal"))
dir.create(paste0(Path,"AutomaticDO\\Misc\\StatisticalTransforms"))
dir.create(paste0(Path,"AutomaticDO\\MonthlyForecasts"))
dir.create(paste0(Path,"AutomaticDO\\MonthlyForecasts\\AccumulatedPrecip"))
dir.create(paste0(Path,"AutomaticDO\\Outcomes"))
dir.create(paste0(Path,"AutomaticDO\\Outcomes\\Classifications"))
dir.create(paste0(Path,"AutomaticDO\\Outcomes\\Prior"))
dir.create(paste0(Path,"AutomaticDO\\Processes"))
dir.create(paste0(Path,"AutomaticDO\\Temp"))
dir.create(paste0(Path,"AutomaticDO\\Temp\\Week1"))
dir.create(paste0(Path,"AutomaticDO\\Temp\\Week2"))
dir.create(paste0(Path,"AutomaticDO\\Temp\\Week3"))
dir.create(paste0(Path,"AutomaticDO\\Wgrib2"))
