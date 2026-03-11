read_data <- function(this_metadata){
  
  options(timeout=300)
  if(!file.exists(paste0(dir_code,"data/global_catch_tunaatlasird_level2_1950_2023_without_geom.csv"))){
    download.file(url = "https://zenodo.org/records/15405414/files/global_catch_tunaatlasird_level2_1950_2023_without_geom.csv?download=1",
                  destfile = paste0(dir_code,"./data/global_catch_tunaatlasird_level2_1950_2023_without_geom.csv"))
  }
  
  if(!file.exists(paste0(dir_code,"data/global_catch_tunaatlasird_level2_1950_2023.qs"))){
    download.file(url = "https://zenodo.org/records/15496164/files/global_catch_tunaatlasird_level2_1950_2023.qs?download=1",
                  destfile = paste0(dir_code,"./data/global_catch_tunaatlasird_level2_1950_2023.qs"))
  }
  
  # if(!file.exists(paste0(dir_code,"data/global_catch_tunaatlasird_level2_1950_2023.qs"))){
  #   download.file(url = "https://zenodo.org/records/15405414/files/global_catch_tunaatlasird_level2_1950_2023.qs?download=1",
  #                 destfile = paste0(dir_code,"./data/global_catch_tunaatlasird_level2_1950_2023.qs"))
  # }
  # 
  if(!file.exists(paste0(dir_code,"data/global_catch_firms_level0_harmonized.parquet"))){
    download.file(url = "https://zenodo.org/records/17707421/files/global_catch_firms_level0_harmonized.parquet?download=1",
                  destfile = paste0(dir_code,"./data/global_catch_firms_level0_harmonized.parquet"))
    
  }
  
  if(!file.exists(paste0(dir_code,"data/global_effort_tunaatlasird_level0_1950_2023.qs"))){
    download.file(url = "https://zenodo.org/records/15496164/files/global_effort_tunaatlasird_level0_1950_2023.qs?download=1",
                  destfile = paste0(dir_code,"./data/global_effort_tunaatlasird_level0_1950_2023.qs"))
    
  }
  
  # LOAD THE WHOLE DATASET
  # load catch spatial data from local files or Zenodo : https://zenodo.org/records/15496164 / Abstract DOI https://doi.org/10.5281/zenodo.1164127
  this_metadata$whole_df <- switch(paste0(this_metadata$variable,"_",this_metadata$level),
                                   "catch_L2"= qs::qread("./data/global_catch_tunaatlasird_level2_1950_2023.qs"),
                                   "catch_L0"= arrow::read_parquet("./data/global_catch_firms_level0_harmonized.parquet"),
                                   # "catch_L2"= read.csv("./data/global_catch_tunaatlasird_level2_1950_2023_without_geom.csv"),
                     # "catch_L2"=qs2::qread("~/Téléchargements/global_catch_tunaatlasird_level2_1950_2023.qs"),
                     "effort_L0"= qs::qread("./data/global_effort_tunaatlasird_level0_1950_2023.qs") %>% mutate_at(vars(gear_type), as.character)
  ) %>% as_tibble()
  
  # TRANSFORM STRING VALUES IN THE ORIGINAL DATAFRAME INTO NUMBERS / Index of codelists
  # SNAPSHOT INITIAL referential data => store dimensions values in a list of dataframes ordered by the most represented
  this_metadata$dim_codes <- dimensions_codes(this_metadata)
  # colnames(this_metadata$dim_codes$new_df)
  
  # Build the spatial grids : all pixels in the bounding box
  
  # lons <- this_metadata$dim_codes$lon %>% dplyr::filter(lon %in% unique(this_metadata$dim_codes$new_df$lon))
  this_metadata$lons <- seq(from=min(unique(this_metadata$dim_codes$new_df$lon)), to=max(unique(this_metadata$dim_codes$new_df$lon)), by=this_metadata$sp_resolution) %>% 
    as.data.frame()  %>% setNames(c("lon"))  %>% mutate(lon_rowid = row_number())

  # lats <- this_metadata$dim_codes$lat %>% dplyr::filter(lat %in% unique(this_metadata$dim_codes$new_df$lat))
  this_metadata$lats <-  seq(from=min(unique(this_metadata$dim_codes$new_df$lat)), to=max(unique(this_metadata$dim_codes$new_df$lat)), by=this_metadata$sp_resolution)%>% 
    as.data.frame()  %>% setNames(c("lat"))  %>% mutate(lat_rowid = row_number())
  
  # Metadata to describe Spatio-temporal extent
  this_metadata$time_coverage_start <- min(this_metadata$dim_codes$times$time_start)
  this_metadata$time_coverage_end <- max(this_metadata$dim_codes$times$time_start)
  # WRITE BOUNDING BOX FOR METADATA / NetCDF file Header
  this_metadata$geospatial_lat_min <- min(this_metadata$lats$lat)
  this_metadata$geospatial_lat_max <- max(this_metadata$lats$lat)
  this_metadata$geospatial_lon_min <- min(this_metadata$lons$lon)
  this_metadata$geospatial_lon_max <- max(this_metadata$lons$lon)
  
  this_metadata$template_filename <- paste0("/Global_Tuna_Atlas_",this_metadata$variable,"_",this_metadata$level,"_",
                                            this_metadata$sp_resolution,"deg_1m","_in_",
                                            paste(this_metadata$catch_unit, collapse = '_and_'),"_",
                                            this_metadata$time_coverage_start,"_",this_metadata$time_coverage_end )
  this_metadata$id <- this_metadata$template_filename
  this_metadata$filename <- this_metadata$template_filename
  
  time_steps <-  sort(unique(this_metadata$dim_codes$times$time_rowid))
  
  empty_time_steps <- vector("numeric", length=length(unique(this_metadata$dim_codes$times$time_rowid)))
  empty_time_steps[] <- NA
  # empty_time_steps
  
  # aaa <- coverages_chunk %>% dplyr::filter(group_id==45) %>% arrange(lon,lat,coverage_id,time_rowid)
  # ddd <- coverages_chunk %>% dplyr::filter(group_id==45) %>% group_by(lon,lat) %>% summarize(nb_steps=n()) %>% arrange(desc(nb_steps))
  # mydf <- head(aaa)
  # for(i in 1:nrow(mydf)){
  #   empty_time_steps[mydf$time_rowid[i]] = mydf$measurement_value[i]
  # }
  # empty_time_steps
  if(this_metadata$variable=="catch"){
    this_metadata$test_df <- this_metadata$dim_codes$new_df  %>% 
      dplyr::left_join(this_metadata$lons) %>% 
      dplyr::left_join(this_metadata$lats) %>% 
      dplyr::left_join(this_metadata$dim_codes$geoms) %>% 
      dplyr::select(c("lon_rowid","lat_rowid","time_rowid","species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_value","measurement_unit","geographic_identifier")) %>% 
      dplyr::rename(lon=lon_rowid,lat=lat_rowid) %>% dplyr::arrange(lon,lat) # %>% dplyr::relocate(geographic_identifier)
    # dplyr::select(c("nb_lines","rowid","lon","lon_rowid","lat","lat_rowid","time_rowid","species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_value"))
    # View(test_df)
    # nrow(test_df)
    # unique(test_df$geographic_identifier)
  }
  if(this_metadata$variable=="effort"){
    this_metadata$test_df <- this_metadata$dim_codes$new_df  %>% 
      dplyr::left_join(this_metadata$lons) %>% 
      dplyr::left_join(this_metadata$lats) %>% 
      dplyr::left_join(this_metadata$dim_codes$geoms) %>% 
      dplyr::select(c("lon_rowid","lat_rowid","time_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_value","measurement_unit","geographic_identifier")) %>% 
      dplyr::rename(lon=lon_rowid,lat=lat_rowid) %>% dplyr::arrange(lon,lat) # %>% dplyr::relocate(geographic_identifier)
  }  
  
  
  
  # catch_array <- array(NA, dim = c(length(lats), length(lons), length(times$time_rowid),
  #                                  length(species$species_rowid),length(gears$gear_rowid),
  #                                  length(fleets$fishing_fleet_rowid),
  #                                  length(modes$fishing_mode_rowid)
  #                                  ),
  #                      dimnames= XXX,
  #                      )
  
  # https://r-spatial.org/book/07-Introsf.html#reading-and-writing-raster-data
  # loaded_data_sf <- this_metadata$dim_codes$new_df %>% st_as_sf() 
  # coverages <- cluster_Coverages(this_metadata$dim_codes$new_df)
  
  return(this_metadata)
}

