dimensions_codes <- function(this_metadata){

  # Debug
  this_df <- this_metadata$whole_df  %>% as_tibble()  %>% 
    dplyr::mutate(geographic_identifier=as.character(geographic_identifier)) %>% 
    dplyr::mutate(time = time_start) #%>% dplyr::select(-c(source_authority,geographic_identifier_nom)) 
  these_geo_ids <- unique(this_df$geographic_identifier)
  
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
  
  if(!file.exists(paste0(dir_code,"data/cl_nc_areas.csv"))){
    download.file(url = "https://github.com/fdiwg/fdi-codelists/raw/main/global/firms/gta/cl_nc_areas.csv",
                  destfile = paste0(dir_code,"./data/cl_nc_areas.csv"))
  }
  
  if(!file.exists(here::here("data/cl_nc_areas_simplified.gpkg"))){
    df_distinct_geom_nominal <- readr::read_csv("./data/cl_nc_areas.csv") %>% 
      dplyr::mutate('geom'= geom_wkt)  %>%
      sf::st_as_sf(wkt="geom",crs=4326) # %>% 
      # st_convex_hull() %>% st_coordinates()
      # st_simplify(dTolerance = 0.5)
    st_write(df_distinct_geom_nominal,dsn = here::here("data/cl_nc_areas_simplified.gpkg"))
    # df_distinct_geom_nominal <- sf::read_sf(here::here("data/cl_nc_areas_simplfied.gpkg"))
  }
  
  if(!file.exists(paste0(dir_code,"data/cl_un_geodata_simplified.gpkg"))){
    download.file(url = "https://github.com/fdiwg/fdi-codelists/raw/refs/heads/main/global/un/cl_un_geodata_simplified.gpkg",
                  destfile = paste0(dir_code,"./data/cl_un_geodata_simplified.gpkg"))
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
  
  if(this_metadata$variable != "nominal_catch"){
    df_geom <- read.csv(paste0(dir_code,"./data/cl_areal_grid.csv"),colClasses=c("character"))  %>% 
      dplyr::mutate(gridtype=GRIDTYPE,geographic_identifier=code)  %>% 
      dplyr::filter(geographic_identifier %in% these_geo_ids)
    
    init_colnames <- colnames(df_geom) 
    
    df_geom_1 <- df_geom %>% dplyr::filter(gridtype == '1deg_x_1deg')  %>% 
      st_as_sf(wkt="geom_wkt", crs=4326)  %>% st_centroid()
    df_geom_5 <- df_geom %>% dplyr::filter(gridtype == '5deg_x_5deg')  %>% 
      st_as_sf(wkt="geom_wkt", crs=4326) #%>% st_combine()
    # https://book.utilitr.org/03_Fiches_thematiques/Fiche_donnees_spatiales.html
    df_1_in_5 <- df_geom_1 %>% sf::st_join(df_geom_5) %>%  as_tibble() %>% 
      dplyr::select(geographic_identifier=code.x,in_geographic_identifier=code.y) 
    df_geom <- df_geom %>% dplyr::left_join(df_1_in_5,by=c("geographic_identifier")) %>% 
      dplyr::select(c(init_colnames,"in_geographic_identifier")) %>% 
       mutate(in_geographic_identifier = case_when(is.na(in_geographic_identifier) ~ geographic_identifier,
                                                             TRUE ~ in_geographic_identifier
                                                             )) %>%
      dplyr::arrange(in_geographic_identifier)
      
  }else{
    df_geom <- sf::read_sf(here::here("data/cl_nc_areas_simplfied.gpkg"))   %>% 
      st_convex_hull() %>% st_centroid %>% 
      dplyr::mutate(gridtype="NAdeg_x_nominal",geographic_identifier=code,
                    lon=st_coordinates(.,crs = 4326)[,"X"],
                    lat=st_coordinates(.,crs = 4326)[,"Y"]) 
  }
   
  cwp_species <- read.csv(paste0(dir_code,"./data/cl_asfis_species_enriched_with_worms.csv"))  %>% 
    dplyr::mutate(species=code,species_label=scientificname) 
  cwp_gears <- read.csv(paste0(dir_code,"./data/cl_isscfg_gear.csv"),colClasses=c("character"))  %>% 
    dplyr::mutate(gear_type=as.character(code),gear_type_label=label) 
  cwp_fleet <- read.csv(paste0(dir_code,"./data/cl_fishing_fleet.csv"))  %>% 
    dplyr::mutate(fishing_fleet=code,fishing_fleet_label=label) 
  # if(!file.exists(paste0(dir_code,"data/fleet_countries.gpkg"))){
  # }else{
    layer_fleet <- sf::st_read("data/cl_un_geodata_simplified.gpkg") %>% 
      #Removing issue with MUS duplicates non consistent for georeg see MUS chagos
      dplyr::filter(!is.null(lbl_en),!grepl("_",iso3cd),objectid!=272) %>% 
      dplyr::filter(st_geometry_type(geom) %in% c("MULTIPOLYGON","POLYGON")) %>%
      # dplyr::filter(!(iso3cd %in% c("ATA","FJI","GRL","RUS",NA))) %>% # issue with S2 only
      dplyr::group_by(iso3cd,georeg) %>% dplyr::mutate(geom = st_union(geom))  %>%
      summarise() %>% dplyr::rename(countryname=iso3cd)
  # }
  cwp_catch_type <- read.csv(paste0(dir_code,"./data/cl_catch_concepts.csv"))  %>% 
    dplyr::mutate(measurement_type=code,measurement_type_label=label) 
  
  
  # df_geom <- read.csv("~/Bureau/CODE/geoflow-tunaatlas/data/cl_areal_grid.csv")  %>% 
  #   filter(GRIDTYPE==this_metadata$grid_resolution) 
  # lons <- sort(unique(df_geom$X)) %>% as.data.frame()  %>% setNames(c("lons"))  %>% mutate(lons_rowid = row_number())
  # lats <- sort(unique(df_geom$Y)) %>% as.data.frame()  %>% setNames(c("lats"))  %>% mutate(lats_rowid = row_number())
  # head(lats)
  # lats <-length(sort(unique(df_geom$Y)))
  
  # Store the different codifications
  if(this_metadata$variable != "nominal_catch"){
    list_dimensions_codes$geoms_whole_init <-  this_df %>% as_tibble() %>%  
      group_by(geographic_identifier) %>% summarise(nb_lines = n())  %>%  
      dplyr::left_join(df_geom) %>% 
      dplyr::mutate(lat=as.numeric(Y_COORD),lon=as.numeric(X_COORD))  %>%  
      dplyr::select(c(geographic_identifier,gridtype,lat,lon,in_geographic_identifier,nb_lines,geom_wkt)) %>% 
      arrange(desc(nb_lines),geographic_identifier)
    
    # alt_df <-  this_df %>% as_tibble() %>%  
    #   group_by(geographic_identifier,measurement_unit) %>% summarise(nb_lines = n())  %>%  
    #   dplyr::left_join(df_geom) %>% 
    #   dplyr::mutate(lat=as.numeric(Y_COORD),lon=as.numeric(X_COORD))  %>%  
    #   dplyr::select(c(measurement_unit,geographic_identifier,gridtype,lat,lon,in_geographic_identifier,nb_lines,geom_wkt)) %>% 
    #   arrange(geographic_identifier,desc(nb_lines))
    
    
    list_dimensions_codes$geoms <- list_dimensions_codes$geoms_whole_init %>% 
      dplyr::filter(gridtype  %in% this_metadata$grid_resolution)
    
    list_dimensions_codes$lon <- list_dimensions_codes$geoms %>% group_by(lon) %>% summarise() %>%
    mutate(lon_rowid = row_number()) %>%  arrange(lon)
    
    list_dimensions_codes$lat <-   list_dimensions_codes$geoms  %>% group_by(lat) %>% summarise() %>%
      mutate(lat_rowid = row_number()) %>%  arrange(lat)
    
    list_dimensions_codes$lon <- seq(from=min(list_dimensions_codes$geoms$lon), to=max(list_dimensions_codes$geoms$lon), by=this_metadata$sp_resolution) %>% 
      as_tibble() %>% setNames(c("lon"))  %>% mutate(lon_rowid = row_number())
    # View(list_dimensions_codes$lon)
    
    # list_dimensions_codes$lat <-   list_dimensions_codes$geoms  %>% group_by(lat) %>% summarise() %>% 
    #   mutate(lat_rowid = row_number()) %>%  arrange(lat) 
    list_dimensions_codes$lat <-  seq(from=min(list_dimensions_codes$geoms$lat), to=max(list_dimensions_codes$geoms$lat), by=this_metadata$sp_resolution)%>% 
      as_tibble()  %>% setNames(c("lat"))  %>% mutate(lat_rowid = row_number())
    
    
 
  }else{
    list_dimensions_codes$geoms <- this_df %>% group_by(geographic_identifier) %>% summarise(nb_lines = n())  %>%  
      arrange(desc(nb_lines),geographic_identifier)  %>% dplyr::left_join(df_geom,by=c("geographic_identifier")) 
    # list_dimensions_codes$lon <- list_dimensions_codes$geoms  %>% group_by(lon) %>% summarise() %>% 
    # mutate(lon_rowid = row_number()) %>%  arrange(lon)
    
    
    list_dimensions_codes$geoms_whole_init <- list_dimensions_codes$geoms
    
    list_dimensions_codes$lon <- list_dimensions_codes$geoms  %>% group_by(lon) %>% summarise() %>%
      mutate(lon_rowid = row_number()) %>%  arrange(lon)
    
    list_dimensions_codes$lat <-   list_dimensions_codes$geoms  %>% group_by(lat) %>% summarise() %>%
      mutate(lat_rowid = row_number()) %>%  arrange(lat)
  }
  
  
  if(!("gridtype" %in% colnames(this_metadata$whole_df))){
    this_df <- this_df  %>% dplyr::left_join(df_geom[,c("geographic_identifier","gridtype")])
  }
  
  # https://forum.posit.co/t/rename-columns-using-vector-of-names/181267
  # orig_names <- colnames(this_df  %>% group_by(time_start,time_end,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines))
  # new_cols <- orig_names[3:length(orig_names)]
  # new_names <- c(orig_names[1:2],paste0("lines_grid",substr(new_cols, 1, 1)))
  new_cols <- unique(this_df$gridtype)
  # new_names <- c(paste0("lines_grid",sub(new_cols, 1, 1)))
  new_names <- c(paste0("lines_grid",sub("deg_x_.*", "", new_cols)))
  # orig_names <- setNames(orig_names, new_names)
  orig_names <- setNames(new_cols, new_names)
  
  # replace_names = c(lines_grid1 = "1deg_x_1deg", lines_grid5="5deg_x_5deg", lines_grid10="10deg_x_10deg")
  # replace_names = c(lines_grid5 ="5deg_x_5deg")
  # ddd %>% rename(new_names=new_cols)
  # ddd %>% setNames(new_names)
  # rename(ddd, all_of(orig_names))
  # rename(ddd, any_of(orig_names))
  
  # list_dimensions_codes$geoms_whole %>% group_by(across(any_of(c("label","geographic_identifier","gridtype","lon","lat","geom_wkt")))) %>% 
  list_dimensions_codes$geoms_whole <- this_df %>% group_by(geographic_identifier,gridtype) %>% 
    summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    rename(any_of(orig_names)) %>% 
    mutate(nb_lines = rowSums(across(new_names),na.rm=T))  %>% 
      dplyr::left_join(list_dimensions_codes$geoms_whole_init) %>% arrange(desc(nb_lines))
  
  list_dimensions_codes$new_geoms_whole <- this_df %>% group_by(geographic_identifier,gridtype,measurement_unit) %>% 
    summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
    mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
    dplyr::select(-c(gridtype,measurement_unit)) %>% 
    rename(gridtype = concat) %>% 
    pivot_wider(names_from = gridtype, values_from = c(nb_lines,total_catch))              %>%
    mutate(nb_lines = rowSums(across(names(.)[grep("nb_lines_",names(.))]),na.rm=T)) %>% 
    mutate(total_catch = rowSums(across(names(.)[grep("total_catch_",names(.))]),na.rm=T)) %>% 
    dplyr::left_join(list_dimensions_codes$geoms_whole_init)  %>% ungroup() %>% arrange(desc(nb_lines)) %>%  
    rename(any_of(setNames(names(.)[grep("nb_lines_",names(.))], sub(".*x_", "lines_grid",names(.)[grep("nb_lines_",names(.))]))))  %>%  
    rename(any_of(setNames(names(.)[grep("total_catch_",names(.))], sub(".*x_", "catch_grid",names(.)[grep("total_catch_",names(.))]))))
  
  

  
  # list_dimensions_codes$times <-  this_df  %>% group_by(time_start,time_end,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>%
  #   rename(any_of(orig_names)) %>% 
  #   # mutate(nb_lines=sum(lines_grid1,lines_grid5,lines_grid10,na.rm=T)) %>%
  #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
  #   ungroup() %>% mutate(time=time_start,
  #                        time_day=as.numeric(julian(as.POSIXct(time, tz = "UTC"), origin = as.Date("1950-01-01"))),
  #                        time_rowid = row_number())  %>%  arrange(time_start)
  # # times <- sort(unique(catch_data_df$time)) %>% as.data.frame() %>% setNames(c("time")) %>% mutate(time_rowid = row_number())
  # # times <- times %>% mutate(time_day=as.numeric(julian(as.POSIXct(times$time, tz = "UTC"), origin = as.Date("1950-01-01"))))
  # 
  list_dimensions_codes$times <-  this_df  %>% group_by(time_start,time_end,gridtype,measurement_unit) %>% 
    summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
    mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
    dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
    rename(gridtype = concat) %>% 
    pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
    ungroup() %>% mutate(time=time_start,
                         time_day=as.numeric(julian(as.POSIXct(time, tz = "UTC"), origin = as.Date("1950-01-01"))),
                         time_rowid = row_number()) %>%  
    rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))  %>% 
    arrange(time_start)

  
  if("species" %in% this_metadata$dims){
    # list_dimensions_codes$species <- this_df  %>% group_by(species,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    #   # mutate(lines_grid1 = ifelse(is.na(`1deg_x_1deg`), 0, `1deg_x_1deg`), lines_grid5 = ifelse(is.na(`5deg_x_5deg`), 0, `5deg_x_5deg`)) %>% 
    #   # mutate(nb_lines=sum(lines_grid1,lines_grid5,na.rm=T)) %>% 
    #   rename(any_of(orig_names)) %>% 
    #   # mutate(across(new_names, sum,na.rm=T,.names = "nb_lines")) %>%  
    #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
    #   dplyr::left_join(cwp_species)  %>% ungroup() %>% arrange(desc(nb_lines),species) %>% mutate(species_rowid = row_number())
    # 
    
    list_dimensions_codes$species <-  this_df %>% group_by(species,gridtype,measurement_unit) %>% 
      summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
      mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
      dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
      rename(gridtype = concat) %>% 
      pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
      mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
      dplyr::left_join(cwp_species)  %>% ungroup() %>% arrange(desc(nb_lines),species) %>% mutate(species_rowid = row_number()) %>%  
      rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))
    
    # toto <- this_df %>% group_by(species,gridtype,measurement_unit) %>%
    #   summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
    #   mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>%
    #   mutate(total_catch = case_when( measurement_unit %in% "no" ~ total_catch*0.015,.default = total_catch) ) %>%
    #   dplyr::select(-c(gridtype,measurement_unit,nb_lines)) %>%
    #   rename(gridtype = concat) %>%
    #   pivot_wider(names_from = gridtype, values_from = total_catch) %>%
    #   mutate(total_catch = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>%
    #   dplyr::left_join(cwp_species)  %>% ungroup() %>% arrange(desc(total_catch),species) %>% mutate(species_rowid = row_number()) %>%
    #   rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))

    # both_toto <- this_df %>% group_by(species,gridtype,measurement_unit) %>%
    #   summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
    #   mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>%
    #   mutate(total_catch = case_when( measurement_unit %in% "no" ~ total_catch*0.015,.default = total_catch) ) %>%
    #   dplyr::select(-c(gridtype,measurement_unit)) %>%
    #   rename(gridtype = concat) %>%
    #   pivot_wider(names_from = gridtype,  values_from=c(nb_lines, total_catch)) %>%
    #   mutate(total_catch = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>%
    #   dplyr::left_join(cwp_species)  %>% ungroup() %>% arrange(desc(total_catch),species) %>% mutate(species_rowid = row_number()) %>%
    #   rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))
    # 
    
  }
  
  
  
  if(this_metadata$variable=="catch" || this_metadata$variable=="nominal_catch"){
    # list_dimensions_codes$measurements <- this_df %>% group_by(measurement_type,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    #   rename(any_of(orig_names)) %>% 
    #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
    #   dplyr::left_join(cwp_catch_type)  %>% ungroup() %>%  arrange(desc(nb_lines)) %>% mutate(catch_type_rowid = row_number())
    # 
    list_dimensions_codes$measurements <-  this_df %>% group_by(measurement_type,gridtype,measurement_unit) %>% 
      summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
      mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
      dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
      rename(gridtype = concat) %>% 
      pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
     # setNames(names(.)[grep("deg_x_",names(.))],sub(".*x_", "",names(.)[grep("deg_x_",names(.))]))
      # rename(any_of(orig_names)) %>% 
      # rename_with( ~ gsub("deg_x_.*", "toto", .x, fixed = TRUE))
      # setNames(paste0("lines_",sub(".*x_", "",names(.)))) %>%  colnames()
  # setNames(paste0("lines_",sub(".*x_", "",names(.)[grep("deg_x_",names(.))]))) %>%  colnames()
  # setNames(paste0("lines_",sub(".*x_", "",grepl("deg_x_",names(.))))) %>%  colnames()
      mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
      dplyr::left_join(cwp_catch_type)  %>% ungroup() %>%  
    arrange(desc(nb_lines)) %>% mutate(catch_type_rowid = row_number()) %>%  
    rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))
  
    
    }

  # list_dimensions_codes$gears <- this_df  %>% group_by(gear_type,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
  #   rename(any_of(orig_names)) %>% 
  #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
  #   dplyr::left_join(cwp_gears)  %>% ungroup() %>%  arrange(desc(nb_lines))  %>% mutate(gear_rowid = row_number())
  # 
  
  list_dimensions_codes$gears <-  this_df %>% group_by(gear_type,gridtype,measurement_unit) %>% 
    summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
    mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
    dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
    rename(gridtype = concat) %>% 
    pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
    dplyr::left_join(cwp_gears)  %>% ungroup() %>% arrange(desc(nb_lines)) %>% mutate(gear_rowid = row_number()) %>%  
    rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))
  
  
  
  
  if(this_metadata$variable!="conversion_factor"){
    # list_dimensions_codes$fleets <- this_df %>% group_by(fishing_fleet,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    #   # mutate(lines_grid1 = ifelse(is.na(`1deg_x_1deg`), 0, `1deg_x_1deg`), lines_grid5 = ifelse(is.na(`5deg_x_5deg`), 0, `5deg_x_5deg`)) %>%
    #   rename(any_of(orig_names)) %>% 
    #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
    #   dplyr::left_join(cwp_fleet)  %>% ungroup() %>%  arrange(desc(nb_lines),fishing_fleet)  %>% mutate(fishing_fleet_rowid = row_number())  %>% 
    #   # Now adding geometries to fishing fleets
    #   # issue withe EUDEU if gsub!!
    #   dplyr::mutate(countryname=sub("EU","",fishing_fleet)) %>% dplyr::left_join(layer_fleet)  %>% st_as_sf()
    # 
    # 
    
    list_dimensions_codes$fleets <-  this_df %>% group_by(fishing_fleet,gridtype,measurement_unit) %>% 
      summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
      mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
      dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
      rename(gridtype = concat) %>% 
      pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
      mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
      dplyr::left_join(cwp_fleet)  %>% ungroup() %>% arrange(desc(nb_lines),fishing_fleet) %>% mutate(fishing_fleet_rowid = row_number()) %>%  
      rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))])))) %>% 
      dplyr::mutate(countryname=sub("EU","",fishing_fleet)) %>% dplyr::left_join(layer_fleet)  %>% st_as_sf()
    
    
    # sf::st_write(obj = df_geoms,paste0("./data/","fleet_countries.gpkg"))
    
    # list_dimensions_codes$modes <- this_df %>% group_by(fishing_mode,gridtype) %>% summarise(nb_lines = n()) %>% pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
    #   rename(any_of(orig_names)) %>% 
    #   mutate(nb_lines = rowSums(across(new_names),na.rm=T)) %>% 
    #   ungroup() %>%  arrange(desc(nb_lines),fishing_mode) %>% mutate(fishing_mode_rowid = row_number())
    # 
    list_dimensions_codes$modes <-  this_df %>% group_by(fishing_mode,gridtype,measurement_unit) %>% 
      summarise(nb_lines = n(),total_catch = sum(measurement_value,na.rm=T)) %>%
      mutate(concat = paste0(gridtype,"-",measurement_unit))  %>% ungroup() %>% 
      dplyr::select(-c(gridtype,measurement_unit,total_catch)) %>% 
      rename(gridtype = concat) %>% 
      pivot_wider(names_from = gridtype, values_from = nb_lines) %>% 
      mutate(nb_lines = rowSums(across(names(.)[grep("deg_x_",names(.))]),na.rm=T)) %>% 
      ungroup() %>% arrange(desc(nb_lines),fishing_mode) %>% mutate(fishing_mode_rowid = row_number()) %>%  
      rename(any_of(setNames(names(.)[grep("deg_x_",names(.))], sub(".*x_", "grid",names(.)[grep("deg_x_",names(.))]))))
    
    }
    

  
  # list_dimensions_codes$measurements <- this_df %>% group_by(measurement,measurement_label,measurement_type,measurement_type_label,measurement_unit,measurement_processing_level,measurement_processing_level_label) %>% summarise(nb_lines = n())  %>%  arrange(desc(nb_lines)) 
  # this_metadata$whole_df %>% group_by(gear_type,gridtype) %>% summarise(nb_lines = n())  %>% group_by(gear_type) %>% summarise(grid = paste0(gridtype,collapse="_"))
  

  # View(all_measurements)
  # View(head(this_df))
  # colnames(this_df)
  # unique(all_measurements$measurement_unit)
  # nrow(this_df)
  # names(list_dimensions_codes) <- this_metadata$dims
  
  if(grepl("catch",this_metadata$variable)){
  # if(this_metadata$variable=="catch" || this_metadata$variable=="nominal_catch"){
    # FILTER NEW DATAFRAME WITH ROWS MATCHING SPATIAL RESOLUTION AND UNIT OF MEASURE
    list_dimensions_codes$new_df <- this_df %>% 
      # dplyr::filter(gridtype  %in% this_metadata$grid_resolution, measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::filter(geographic_identifier  %in%  unique(list_dimensions_codes$geoms$geographic_identifier), measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::left_join(as_tibble(list_dimensions_codes$geoms),by=c("geographic_identifier")) %>% 
      dplyr::select(c(this_metadata$dims,geographic_identifier, measurement_value,measurement_unit)) %>% 
      dplyr::left_join(list_dimensions_codes$times) %>% 
      dplyr::left_join(list_dimensions_codes$species[,c("species", "species_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$gears[,c("gear_type", "gear_rowid")]) %>% 
      dplyr::left_join(as_tibble(list_dimensions_codes$fleets[, c("fishing_fleet", "fishing_fleet_rowid")]),by=c("fishing_fleet")) %>% 
      dplyr::left_join(list_dimensions_codes$modes[,c("fishing_mode", "fishing_mode_rowid")]) %>% 
      # dplyr::select(c(lat,lon,time_rowid,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value))  %>%
      dplyr::group_by(geographic_identifier,lat,lon,time_rowid,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value,measurement_unit)  %>%
      summarise(nb_lines = n()) %>% ungroup() %>%  arrange(desc(nb_lines),time_rowid,lat,lon) %>% mutate(rowid = row_number())
    # %>%   dplyr::filter(gridtype==this_metadata$grid_resolution , measurement_unit==this_metadata$catch_unit)
  }
  if(this_metadata$variable=="effort"){
    # FILTER NEW DATAFRAME WITH ROWS MATCHING SPATIAL RESOLUTION AND UNIT OF MEASURE
    list_dimensions_codes$new_df <- this_df %>% 
      # dplyr::filter(gridtype  %in% this_metadata$grid_resolution, measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::filter(geographic_identifier  %in%  unique(list_dimensions_codes$geoms$geographic_identifier)) %>%
      dplyr::left_join(as_tibble(list_dimensions_codes$geoms),by=c("geographic_identifier")) %>% 
      dplyr::select(c(this_metadata$dims, measurement_value,measurement_unit)) %>% 
      dplyr::left_join(list_dimensions_codes$times) %>% 
      dplyr::left_join(list_dimensions_codes$gears[,c("gear_type", "gear_rowid")]) %>% 
      dplyr::left_join(as_tibble(list_dimensions_codes$fleets[, c("fishing_fleet", "fishing_fleet_rowid")]),by=c("fishing_fleet")) %>% 
      dplyr::left_join(list_dimensions_codes$modes[,c("fishing_mode", "fishing_mode_rowid")]) %>% 
      dplyr::group_by(lat,lon,time_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_value,measurement_unit)  %>%
      summarise(nb_lines = n()) %>% ungroup() %>%  arrange(desc(nb_lines),time_rowid,lat,lon) %>% mutate(rowid = row_number())
    # %>%   dplyr::filter(gridtype==this_metadata$grid_resolution , measurement_unit==this_metadata$catch_unit)
  }
  if(this_metadata$variable=="conversion_factor"){
    # FILTER NEW DATAFRAME WITH ROWS MATCHING SPATIAL RESOLUTION AND UNIT OF MEASURE
    list_dimensions_codes$new_df <- this_df %>% 
      # dplyr::filter(gridtype  %in% this_metadata$grid_resolution, measurement_unit %in% this_metadata$catch_unit) %>%
      dplyr::filter(geographic_identifier  %in%  unique(list_dimensions_codes$geoms$geographic_identifier)) %>%
      dplyr::left_join(as_tibble(list_dimensions_codes$geoms),by=c("geographic_identifier")) %>% 
      dplyr::select(c(this_metadata$dims, measurement_value,measurement_unit)) %>% 
      dplyr::left_join(list_dimensions_codes$times) %>%  
      dplyr::left_join(list_dimensions_codes$species[,c("species", "species_rowid")]) %>% 
      dplyr::left_join(list_dimensions_codes$gears[,c("gear_type", "gear_rowid")]) %>% 
      dplyr::group_by(lat,lon,time_rowid,species_rowid,gear_rowid,measurement_value,measurement_unit)  %>%
      summarise(nb_lines = n()) %>% ungroup() %>% arrange(desc(nb_lines),time_rowid,lat,lon) %>% mutate(rowid = row_number())
    # %>%   dplyr::filter(gridtype==this_metadata$grid_resolution , measurement_unit==this_metadata$catch_unit)
  }

    
  return(list_dimensions_codes) 
}