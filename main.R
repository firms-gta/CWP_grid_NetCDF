# parallel::stopCluster(cl)
# rm(cl)
############################### Set packages and working directories ############################### 
library(dplyr)
library(readr)
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
source("./R/plot_page_summary.R")
source("./R/plot_multipanel.R")
source("./R/plot_map.R")
sf_use_s2(FALSE)
############################### CATCH ############################### 
# list_DOIs <- here::here("data/DOI.csv")
this_data <- NULL
init_metadata <- list()
coverages <- NULL
dim_codes <-list()
# init_metadata$sp_resolution <- c("nominal")
# init_metadata$sp_resolution <- 5
init_metadata$sp_resolution <- c(1,5)
init_metadata$grid_resolution <- paste0(init_metadata$sp_resolution,"deg_x_",
                                        init_metadata$sp_resolution,"deg")
init_metadata$catch_unit <- c("t","no")
init_metadata$effort_unit <- c("SUC.SETS","SUC.D.FI","NO.FADS.VIS","Hours.FSC","Hours.FAD",
                               "Hours.STD","D.FISH.G","LINE.DAYS","NO.NETS","KM.SETS","TRAP D",
                               "NO.TRAPS","NO.MTZAS","N.POLE-D","FHOURS","HOURS","FDAYS","DAYS",
                               "HRSRH","SETS","TRIPS","NETS","BOATS","HOOKS","MD","LINES")
# init_metadata$catch_unit <- c("t")
# Set the granularity for files generation :  c("coverage","core","coverage_group","all")
init_metadata$file_unit <- "all"
# init_metadata$level <- "L2"
init_metadata$level <- "L0"
#set filters for spatial resolution and measurement unit to be kept : GTA_5_Deg_in_t => OUI(20Cores), GTA_1_Deg_in_no => OUI, 5Deg_no => ???, 1Deg_t => ???, 
# init_metadata$variable <- c("catch","effort")
# init_metadata$variable <- "catch"
init_metadata$variable <- "nominal_catch"
init_metadata$variable <- "conversion_factor"
# init_metadata$variable <- "effort"
#set dimensions to be kept


init_metadata$dims <- switch(init_metadata$variable,
                             "nominal_catch"= c("lat","lon","time","species","gear_type","fishing_fleet","fishing_mode"),
                             "catch"= c("lat","lon","time","species","gear_type","fishing_fleet","fishing_mode"),
                             "effort"=c("lat","lon","time","gear_type","fishing_fleet","fishing_mode"),
                             "conversion_factor"=c("lat","lon","time","species","gear_type")
)
init_metadata$doi <- "https://doi.org/10.5281/zenodo.15496164"
col_names <- c("parent_dataset","dataset_name","nb_rows_raw_data","nb_rows_coverages","nb_coverages","nb_groups")
df_summary <- data.frame(matrix(ncol = 6, nrow = 0))
colnames(df_summary) <- col_names

start_time <- Sys.time()
print(start_time)
print("Start loop")

datasets_df <- read.csv("./data/all_datasets.csv")[2:2,]
# datasets_df <- read.csv("./data/all_datasets.csv")[2:8,]
colnames(datasets_df)

for (i in 1:nrow(datasets_df)){
  # print(datasets_df[i,])
  init_metadata$variable <- datasets_df[i,1]
  init_metadata$level <- datasets_df[i,2]
  init_metadata$sp_resolution <- as.numeric(strsplit(x = datasets_df[i,3],",")[[1]])
  # init_metadata$sp_resolution <- c(1,5)
  init_metadata$grid_resolution <- paste0(init_metadata$sp_resolution,"deg_x_",
                                          init_metadata$sp_resolution,"deg")
  
  init_metadata$dims <- switch(init_metadata$variable,
                               "nominal_catch"= c("lat","lon","time","species","gear_type","fishing_fleet","fishing_mode"),
                               "catch"= c("lat","lon","time","species","gear_type","fishing_fleet","fishing_mode"),
                               "effort"=c("lat","lon","time","gear_type","fishing_fleet","fishing_mode"),
                               "conversion_factor"=c("lat","lon","time","species","gear_type")
  )


# if(init_metadata$variable  %in% c("nominal_catch")){
  # this_metadata <- read_data(this_metadata)
  # df <- this_metadata$dim_codes$fleets %>% 
  #   # issue withe EUDEU if gsub!!
  #   dplyr::mutate(countryname=sub("EU","",fishing_fleet)) %>%
  #   dplyr::select(-c(`1deg_x_1deg`,`5deg_x_5deg`)) %>% arrange(countryname,nb_lines)
  # View(df)
  # colnames(df)
  # 
  # layer <- sf::st_read("data/cl_un_geodata_simplified.gpkg") %>% 
  #   #Removing issue with MUS duplicates non consistent for georeg see MUS chagos
  #   dplyr::filter(!is.null(lbl_en),!grepl("_",iso3cd),objectid!=272) %>% 
  #   dplyr::filter(st_geometry_type(geom) %in% c("MULTIPOLYGON","POLYGON")) %>%
  #   # dplyr::filter(!(iso3cd %in% c("ATA","FJI","GRL","RUS",NA))) %>% # issue with S2 only
  #   dplyr::group_by(iso3cd,georeg) %>% dplyr::mutate(geom = st_union(geom))  %>%
  #   summarise() %>% dplyr::rename(countryname=iso3cd)
  # df_geoms <-  df %>% dplyr::left_join(layer)  %>% st_as_sf()
  
  # sf::st_write(obj = df_geoms,paste0("./data/","fleet_countries.gpkg"))
# }


if(length(init_metadata$sp_resolution) >= 1){
  
  for(res in init_metadata$sp_resolution){
    this_metadata <- list()
    this_metadata <- init_metadata
    this_metadata$sp_resolution <- res
    this_metadata$grid_resolution <- paste0(res,"deg_x_",res,"deg")
    # Read dataset and transform it to prepare NetCDF conversion
    setwd(dir_code)
    print("Read dataset")
    this_metadata <- read_data(this_metadata)
    # activated or not
    print("Calculate coverages")
    this_metadata$coverages_df <- cluster_Coverages(this_metadata$test_df)
    #Store basic metadata
    print("Store basic metadata")
    new_row <- c(parent_dataset=paste0(this_metadata$variable,"_",this_metadata$level),
                 dataset_name=paste0(this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_')),
                 nb_rows_raw_data=nrow(this_metadata$whole_df),
                 nb_rows_coverages=nrow(this_metadata$coverages_df$coverages),
                 nb_coverages=length(unique(this_metadata$coverages_df$coverages$coverage_id)),
                 nb_groups=length(unique(this_metadata$coverages_df$coverages$group_id)))
    df_summary[nrow(df_summary) + 1,] <- new_row
    print(new_row)
    this_metadata$summary <- df_summary[nrow(df_summary),]
    # Create multi panel plots
    print("Create multi panel plots")
    # plot_page_summary(metadata=this_metadata)
    
    #Create repository if necessary and write NetCDF files
    this_metadata$dir_NetCDF <- paste0(dir_data,"/GTA_",this_metadata$variable,"_",this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_'))
    if(!dir.exists(this_metadata$dir_NetCDF)){
      dir.create(path = this_metadata$dir_NetCDF)
    }
    setwd(this_metadata$dir_NetCDF)
    # if(init_metadata$variable != "nominal_catch"){
    #   write_NetCDF(this_metadata,nb_cores=30)
    # }
    
    setwd(dir_code)
    this_file_name=paste0("./data/",this_metadata$variable,"_",this_metadata$level,"_",this_metadata$sp_resolution,"_Deg_in_",paste(this_metadata$catch_unit, collapse = '_and_'),".RDS")
    if(!file.exists(this_file_name)){
      saveRDS(object = this_metadata,file=this_file_name)
    }

  }
  # Record some basic information
  # this_metadata$sp_resolution <- init_metadata$sp_resolution
  if(length(unique(metadata$dim_codes$geoms_whole$gridtype)) > 1){
    this_metadata$sp_resolution <- c(paste0("",sub("deg_x_.*", "", unique(metadata$dim_codes$geoms_whole$gridtype))))
    this_metadata$id <- paste0(this_metadata$id,"_all")
    plot_page_summary(metadata=this_metadata)
    }
}
    
df_summary
setwd(dir_code)
write.csv(x = df_summary,file=paste0("./data/df_summary_",this_metadata$variable,"_",this_metadata$level,".csv"))
saveRDS(object = df_summary,file=paste0("./data/df_summary_",this_metadata$variable,"_",this_metadata$level,".RDS"))

end_time <- Sys.time()
print(end_time-start_time)
}