############################### Set packages and working directories ############################### 
library(dplyr)
library(parallel)
library(foreach)
library(doParallel)
library(raster)
library(sf)
# library(RNetCDF)
library(ncdf4)
library(tidyr)
library(gdalUtilities)
library(ggplot2)
# dir_data <- "~/data"
dir_data <- "/tmp"
dir_code <- paste0("~/Bureau/CODE/CWP_grid_NetCDF/")
setwd(dir_code)
############################### Source code ###############################
source("./R/read_data.R")
source("./R/dimensions_codes.R")
source("./R/create_NetCDF_ncdf4.R")
source("./R/split_NetCDF.R")
source("./R/cluster_Coverages.R")
source("./R/write_NetCDF.R")
############################### CATCH ############################### 
# list_DOIs <- here::here("data/DOI.csv")
this_data <- NULL
init_metadata <- list()
coverages <- NULL
dim_codes <-list()
init_metadata$sp_resolution <- c(1,5)
# init_metadata$sp_resolution <- 5
# init_metadata$sp_resolution <- c(1,5)
init_metadata$grid_resolution <- paste0(init_metadata$sp_resolution,"deg_x_",
                                        init_metadata$sp_resolution,"deg")
# init_metadata$catch_unit <- c("t","no")
init_metadata$effort_unit <- c("SUC.SETS","SUC.D.FI","NO.FADS.VIS","Hours.FSC","Hours.FAD",
                               "Hours.STD","D.FISH.G","LINE.DAYS","NO.NETS","KM.SETS","TRAP D",
                               "NO.TRAPS","NO.MTZAS","N.POLE-D","FHOURS","HOURS","FDAYS","DAYS",
                               "HRSRH","SETS","TRIPS","NETS","BOATS","HOOKS","MD","LINES")
# init_metadata$catch_unit <- c("t")
# Set the granularity for files generation :  c("coverage","core","coverage_group","both")
init_metadata$file_unit <- "both"
# init_metadata$level <- "L2"
init_metadata$level <- "L0"
#set filters for spatial resolution and measurement unit to be kept : GTA_5_Deg_in_t => OUI(20Cores), GTA_1_Deg_in_no => OUI, 5Deg_no => ???, 1Deg_t => ???, 
# init_metadata$variable <- c("catch","effort")
# init_metadata$variable <- "catch"
init_metadata$variable <- "effort"
#set dimensions to be kept




init_metadata$dims <- switch(init_metadata$variable,
                             "catch"= c("lat","lon","time","species","gear_type","fishing_fleet","fishing_mode"),
                             "effort"=c("lat","lon","time","gear_type","fishing_fleet","fishing_mode")
)
init_metadata$doi <- "https://doi.org/10.5281/zenodo.15496164"
col_names <- c("parent_dataset","dataset_name","nb_rows_raw_data","nb_rows_coverages","nb_coverages","nb_groups")
df_summary <- data.frame(matrix(ncol = 6, nrow = 0))
colnames(df_summary) <- col_names

start_time <- Sys.time()
print(start_time)
print("Start loop")


if(length(init_metadata$sp_resolution) > 1){
  for(res in init_metadata$sp_resolution){
    this_metadata <- list()
    this_metadata <- init_metadata
    this_metadata$sp_resolution <- res
    this_metadata$grid_resolution <- paste0(res,"deg_x_",res,"deg")
    # Read dataset and transform it to prepare NetCDF conversion
    setwd(dir_code)
    this_metadata <- read_data(this_metadata)
    this_metadata$coverages_df <- cluster_Coverages(this_metadata$test_df)
    #Store basic metadata
    new_row <- c(parent_dataset=paste0(this_metadata$variable,"_",this_metadata$level),
                 dataset_name=paste0(this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_')),
                 nb_rows_raw_data=nrow(this_metadata$whole_df),
                 nb_rows_coverages=nrow(this_metadata$coverages_df$coverages),
                 nb_coverages=length(unique(this_metadata$coverages_df$coverages$coverage_id)),
                 nb_groups=length(unique(this_metadata$coverages_df$coverages$group_id)))
    df_summary[nrow(df_summary) + 1,] <- new_row
    print(new_row)
    
    #Create repository if necessary and write NetCDF files
    this_metadata$dir_NetCDF <- paste0(dir_data,"/GTA_",this_metadata$variable,"_",this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_'))
    if(!dir.exists(this_metadata$dir_NetCDF)){
      dir.create(path = this_metadata$dir_NetCDF)
    }
    setwd(this_metadata$dir_NetCDF)
    write_NetCDF(this_metadata,nb_cores=10)
    setwd(dir_code)
    saveRDS(object = this_metadata,file=paste0("./data/",this_metadata$variable,"_",this_metadata$level,"_",this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_'),".RDS"))
    # Record some basic information
  }
}
df_summary
setwd(dir_code)
write.csv(x = df_summary,file=paste0("./data/df_summary_",this_metadata$variable,"_",this_metadata$level,".csv"))
saveRDS(object = df_summary,file=paste0("./data/df_summary_",this_metadata$variable,"_",this_metadata$level,".RDS"))

end_time <- Sys.time()
print(end_time-start_time)