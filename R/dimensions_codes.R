dimensions_codes <- function(this_metadata){

  # Debug
  this_df <- this_metadata$whole_df  %>% as_tibble()  %>% dplyr::mutate(geographic_identifier=as.character(geographic_identifier)) %>% 
    dplyr::mutate(time = time_start) #%>% dplyr::select(-c(source_authority,geographic_identifier_nom)) 
  
  #initialize the list which will store all codes
  list_dimensions_codes <- list()
  
  # URL of the second ZIP file to download
  # zip_url <- "https://github.com/fdiwg/fdi-codelists/raw/main/global/cwp/cl_areal_grid.zip"
  # zip_destfile <- "cl_areal_grid.zip"
  # csv_file_in_zip <- "data/cl_areal_grid.csv" # Specify expected CSV file inside
  
  
  
  options(timeout=300)
  
  if(!file.exists(paste0(dir_code,"data/cl_areal_grid.csv"))){
    download.file(url = "https://github.com/fdiwg/fdi-codelists/raw/main/global/cwp/cl_areal_grid.zip",
                  destfile = paste0(dir_code,"./data/cl_areal_grid.zip"))
    unzip(paste0(dir_code,"./data/cl_areal_grid.zip"), exdir = paste0(dir_code,"./data"))
  }
  
  if(!file.exists(paste0(dir_code,"data/cl_asfis_species_enriched_with_worms.csv"))){
    download.file(url = "https://raw.githubusercontent.com/fdiwg/fdi-codelists/refs/heads/main/global/cwp/cl_asfis_species_enriched_with_worms.csv",
                  destfile = paste0(dir_code,"./data/cl_asfis_species_enriched_with_worms.csv"))
  }
  if(!file.exists(paste0(dir_code,"data/cl_isscfg_gear.csv"))){
    download.file(url = "https://raw.githubusercontent.com/fdiwg/fdi-codelists/main/global/cwp/cl_isscfg_gear.csv",
                  destfile = paste0(dir_code,"./data/cl_isscfg_gear.csv"))
  }
  if(!file.exists(paste0(dir_code,"data/cl_fishing_fleet.csv"))){
    download.file(url = "https://raw.githubusercontent.com/fdiwg/fdi-codelists/main/global/firms/gta/cl_fishing_fleet.csv",
                  destfile = paste0(dir_code,"./data/cl_fishing_fleet.csv"))
  }
  if(!file.exists(paste0(dir_code,"data/cl_catch_concepts.csv"))){
    download.file(url = "https://raw.githubusercontent.com/fdiwg/fdi-codelists/refs/heads/main/global/cwp/cl_catch_concepts.csv",
                  destfile = paste0(dir_code,"./data/cl_catch_concepts.csv"))
  }
  
  df_geom <- read.csv(paste0(dir_code,"./data/cl_areal_grid.csv"),colClasses=c("character"))  %>% 
    dplyr::mutate(gridtype=GRIDTYPE,geographic_identifier=code) 
  cwp_species <- read.csv(paste0(dir_code,"./data/cl_asfis_species_enriched_with_worms.csv"))  %>% 
    dplyr::mutate(species=code,species_label=scientificname) 
  cwp_gears <- read.csv(paste0(dir_code,"./data/cl_isscfg_gear.csv"),colClasses=c("character"))  %>% 
    dplyr::mutate(gear_type=as.character(code),gear_type_label=label) 
  cwp_fleet <- read.csv(paste0(dir_code,"./data/cl_fishing_fleet.csv"))  %>% 
    dplyr::mutate(fishing_fleet=code,fishing_fleet_label=label) 
  cwp_catch_type <- read.csv(paste0(dir_code,"./data/cl_catch_concepts.csv"))  %>% 
    dplyr::mutate(measurement_type=code,measurement_type_label=label) 
  
  
  # df_geom <- read.csv("~/Bureau/CODE/geoflow-tunaatlas/data/cl_areal_grid.csv")  %>% 
  #   filter(GRIDTYPE==this_metadata$grid_resolution) 
  # lons <- sort(unique(df_geom$X)) %>% as.data.frame()  %>% setNames(c("lons"))  %>% mutate(lons_rowid = row_number())
  # lats <- sort(unique(df_geom$Y)) %>% as.data.frame()  %>% setNames(c("lats"))  %>% mutate(lats_rowid = row_number())
  # head(lats)
  # lats <-length(sort(unique(df_geom$Y)))
  
  # Store the different codifications
  list_dimensions_codes$geoms <-  this_df %>% group_by(geographic_identifier) %>% summarise(nb_lines = n())  %>%  
    arrange(desc(nb_lines),geographic_identifier)  %>% dplyr::left_join(df_geom) %>% 
    dplyr::filter(gridtype  %in% this_metadata$grid_resolution) %>% dplyr::mutate(lat=as.numeric(Y_COORD),lon=as.numeric(X_COORD))  %>%  
    dplyr::select(c(geographic_identifier,lat,lon))
  
  # list_dimensions_codes$lon <- list_dimensions_codes$geoms  %>% group_by(lon) %>% summarise() %>% 
    # mutate(lon_rowid = row_number()) %>%  arrange(lon)
  list_dimensions_codes$lon <- seq(from=min(list_dimensions_codes$geoms$lon), to=max(list_dimensions_codes$geoms$lon), by=this_metadata$sp_resolution) %>% 
    as_tibble() %>% setNames(c("lon"))  %>% mutate(lon_rowid = row_number())
  # View(list_dimensions_codes$lon)
  
  # list_dimensions_codes$lat <-   list_dimensions_codes$geoms  %>% group_by(lat) %>% summarise() %>% 
  #   mutate(lat_rowid = row_number()) %>%  arrange(lat) 
  list_dimensions_codes$lat <-  seq(from=min(list_dimensions_codes$geoms$lat), to=max(list_dimensions_codes$geoms$lat), by=this_metadata$sp_resolution)%>% 
    as_tibble()  %>% setNames(c("lat"))  %>% mutate(lat_rowid = row_number())
    
  # lons <- seq(from=min(catch_data_df$lon), to=max(catch_data_df$lon), by=this_metadata$sp_resolution) %>% as.data.frame()  %>% setNames(c("lon"))  %>% mutate(lon_rowid = row_number())
  # lats <- seq(from=min(catch_data_df$lat), to=max(catch_data_df$lat), by=this_metadata$sp_resolution) %>% as.data.frame()  %>% setNames(c("lat"))  %>% mutate(lat_rowid = row_number())
  
  list_dimensions_codes$times <-  this_df  %>% group_by(time_start,time_end) %>% summarise(nb_lines = n()) %>% 
    ungroup() %>% mutate(time=time_start,
                         time_day=as.numeric(julian(as.POSIXct(time, tz = "UTC"), origin = as.Date("1950-01-01"))),
                         time_rowid = row_number())  %>%  arrange(time_start)
  # times <- sort(unique(catch_data_df$time)) %>% as.data.frame() %>% setNames(c("time")) %>% mutate(time_rowid = row_number())
  # times <- times %>% mutate(time_day=as.numeric(julian(as.POSIXct(times$time, tz = "UTC"), origin = as.Date("1950-01-01"))))
  
  if("species" %in% this_metadata$dims){
    list_dimensions_codes$species <- this_df  %>% group_by(species) %>% summarise(nb_lines = n()) %>% 
      dplyr::left_join(cwp_species)  %>%ungroup() %>% arrange(desc(nb_lines),species) %>% mutate(species_rowid = row_number())
  }

  list_dimensions_codes$gears <- this_df  %>% group_by(gear_type) %>% summarise(nb_lines = n()) %>% 
    dplyr::left_join(cwp_gears)  %>% ungroup() %>%  arrange(desc(nb_lines))  %>% mutate(gear_rowid = row_number())
  list_dimensions_codes$fleets <- this_df %>% group_by(fishing_fleet) %>% summarise(nb_lines = n()) %>% 
    dplyr::left_join(cwp_fleet)  %>% ungroup() %>%  arrange(desc(nb_lines),fishing_fleet)  %>% mutate(fishing_fleet_rowid = row_number())  
  list_dimensions_codes$modes <- this_df %>% group_by(fishing_mode) %>% summarise(nb_lines = n()) %>% 
    ungroup() %>%  arrange(desc(nb_lines),fishing_mode) %>% mutate(fishing_mode_rowid = row_number())
  # list_dimensions_codes$measurements <- this_df %>% group_by(measurement,measurement_label,measurement_type,measurement_type_label,measurement_unit,measurement_processing_level,measurement_processing_level_label) %>% summarise(nb_lines = n())  %>%  arrange(desc(nb_lines)) 

  if(this_metadata$variable=="catch"){
    list_dimensions_codes$species <- this_df  %>% group_by(species) %>% summarise(nb_lines = n()) %>% 
      dplyr::left_join(cwp_species)  %>%ungroup() %>% arrange(desc(nb_lines),species) %>% mutate(species_rowid = row_number())
    
    list_dimensions_codes$measurements <- this_df %>% group_by(measurement_type) %>% summarise(nb_lines = n()) %>% 
      dplyr::left_join(cwp_catch_type)  %>% ungroup() %>%  arrange(desc(nb_lines)) %>% mutate(catch_type_rowid = row_number())
  }
  # View(all_measurements)
  # View(head(this_df))
  # colnames(this_df)
  # unique(all_measurements$measurement_unit)
  # nrow(this_df)
  # names(list_dimensions_codes) <- this_metadata$dims
  
  
  if(this_metadata$variable=="catch"){
    # FILTER NEW DATAFRAME WITH ROWS MATCHING SPATIAL RESOLUTION AND UNIT OF MEASURE
    list_dimensions_codes$new_df <- this_df %>% 
      # dplyr::filter(gridtype  %in% this_metadata$grid_resolution, measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::filter(geographic_identifier  %in%  unique(list_dimensions_codes$geoms$geographic_identifier), measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::left_join(list_dimensions_codes$geoms) %>% 
      dplyr::select(c(this_metadata$dims, measurement_value,measurement_unit)) %>% 
      dplyr::left_join(list_dimensions_codes$times) %>% 
      dplyr::left_join(list_dimensions_codes$species[, c("species", "species_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$gears[, c("gear_type", "gear_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$fleets[, c("fishing_fleet", "fishing_fleet_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$modes[, c("fishing_mode", "fishing_mode_rowid")]) %>% 
      # dplyr::select(c(lat,lon,time_rowid,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value))  %>%
      dplyr::group_by(lat,lon,time_rowid,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value,measurement_unit)  %>%
      summarise(nb_lines = n()) %>% ungroup() %>%  arrange(desc(nb_lines),time_rowid,lat,lon) %>% mutate(rowid = row_number())
    # %>%   dplyr::filter(gridtype==this_metadata$grid_resolution , measurement_unit==this_metadata$catch_unit)
  }
  if(this_metadata$variable=="effort"){
    # FILTER NEW DATAFRAME WITH ROWS MATCHING SPATIAL RESOLUTION AND UNIT OF MEASURE
    list_dimensions_codes$new_df <- this_df %>% 
      # dplyr::filter(gridtype  %in% this_metadata$grid_resolution, measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::filter(geographic_identifier  %in%  unique(list_dimensions_codes$geoms$geographic_identifier)) %>%
      dplyr::left_join(list_dimensions_codes$geoms) %>% 
      dplyr::select(c(this_metadata$dims, measurement_value,measurement_unit)) %>% 
      dplyr::left_join(list_dimensions_codes$times) %>% 
      dplyr::left_join(list_dimensions_codes$gears[, c("gear_type", "gear_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$fleets[, c("fishing_fleet", "fishing_fleet_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$modes[, c("fishing_mode", "fishing_mode_rowid")]) %>% 
      dplyr::group_by(lat,lon,time_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value,measurement_unit)  %>%
      summarise(nb_lines = n()) %>% ungroup() %>%  arrange(desc(nb_lines),time_rowid,lat,lon) %>% mutate(rowid = row_number())
    # %>%   dplyr::filter(gridtype==this_metadata$grid_resolution , measurement_unit==this_metadata$catch_unit)
  }
  

    
  return(list_dimensions_codes) 
}