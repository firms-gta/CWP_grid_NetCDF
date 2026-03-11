create_NetCDF_ncdf4 <- function(metadata,lats,lons,times,species,gears,fleets,modes,unlim=FALSE){
  
  ncfilename <- metadata$filename
  #https://search.r-project.org/CRAN/refmans/RNetCDF/html/var.put.nc.html
  # https://cfconventions.org/Data/cf-conventions/cf-conventions-1.8/cf-conventions.html#taxon-names-and-identifiers
  library(ncdf4)
  #print(paste('Creating NetCDF file ',ncfilename,':', sep=''))
  
  # Dimensions and coordinate variables
  if("time" %in% metadata$dims){
    dim_time <- ncdf4::ncdim_def(name="time",longname="time",vals=times,units="days since 1950-01-01 00:00:00",unlim=unlim,calendar="standard",create_dimvar=TRUE)
  }
  if("lon" %in% metadata$dims){
    dim_lon <- ncdf4::ncdim_def(name="lon",longname="longitude",vals=lons,units="degrees_east",create_dimvar=TRUE)
  }
  if("lat" %in% metadata$dims){
    dim_lat <- ncdf4::ncdim_def(name="lat",longname="latitude",vals=lats,units="degrees_north",create_dimvar=TRUE)
  }
  if("species" %in% metadata$dims){
    dim_species <- ncdf4::ncdim_def(name="species",longname="species",vals=species,units="species_code",create_dimvar=TRUE)
  }
  if("gear_type" %in% metadata$dims){
    dim_gear <- ncdf4::ncdim_def(name="gear",longname="fishing_gear",vals=gears,units="gear_code",create_dimvar=TRUE)
  }
  if("fishing_fleet" %in% metadata$dims){
    dim_fleet <- ncdf4::ncdim_def(name="fleet",longname="fishing_fleet",vals=fleets,units="fishing_fleet_code",create_dimvar=TRUE)
  }
  if("fishing_mode" %in% metadata$dims){
    dim_mode <- ncdf4::ncdim_def(name="mode",longname="fishing_mode",vals=modes,units="fishing_mode_code",create_dimvar=TRUE)
  }
  # listDims <- list(dim_time,dim_lon,dim_lat,dim_species,dim_gear,dim_fleet,dim_mode)
  if(metadata$file_unit == "coverage"){
    if(metadata$variable == "effort"){
      listDims <- list(dim_lon,dim_lat,dim_time,dim_gear,dim_fleet,dim_mode)
    }else {
      listDims <- list(dim_lon,dim_lat,dim_time,dim_species,dim_gear,dim_fleet,dim_mode)
      # listDims <- list(dim_species,dim_gear,dim_fleet,dim_mode,dim_time,dim_lat,dim_lon)
    }
  }else{
    if(metadata$variable == "effort"){
      listDims <- list(dim_gear,dim_fleet,dim_mode,dim_time,dim_lon,dim_lat)
    }else{
      listDims <- list(dim_species,dim_gear,dim_fleet,dim_mode,dim_time,dim_lon,dim_lat)
    }
    }


  
  # var_lon <- ncdf4::ncvar_def(name="lon",units="degrees_east",dim=dim_lon,compression=1,prec = "float")
  # var_lat <- ncdf4::ncvar_def(name="lat",units="degrees_north",dim=dim_lat,compression=1,prec = "float")
  # var_time <- ncdf4::ncvar_def(name="time",units="days since 1950-01-01 00:00:00",dim=dim_time,compression=1,shuffle=TRUE, prec = "integer")
  # var_species <- ncdf4::ncvar_def(name="species",units="species_code",dim=dim_species,compression=1,shuffle=TRUE, prec = "integer")
  # var_gear <- ncdf4::ncvar_def(name="gear",units="gear_code",dim=dim_gear,compression=1,shuffle=TRUE, prec = "integer")
  # var_fleet <- ncdf4::ncvar_def(name="fleet",units="fishing_fleet_code",dim=dim_fleet,compression=1,shuffle=TRUE, prec = "integer")
  # var_mode <- ncdf4::ncvar_def(name="mode",units="fishing_mode_code",dim=dim_mode,compression=1,shuffle=TRUE, prec = "integer")
  var_crs <- ncdf4::ncvar_def(name = "crs", units = "", dim = list(),prec = "integer")
  # Data variable with N dimensions
  # var_main <-ncdf4::ncvar_def(name = "measurement_value",units = "tons", dim = list(dim_mode,dim_fleet,dim_gear,dim_species,dim_lat,dim_lon,dim_time), prec = "float",compression = 1)
  if(metadata$variable == "effort"){
    var_effort <-ncdf4::ncvar_def(name = "effort",units = "numbers", dim = listDims, prec = "float",compression = 1)
    variables <- list(var_crs,var_effort)
    
  }else{
    var_catch_t <-ncdf4::ncvar_def(name = "catch_t",units = "tons", dim = listDims, prec = "float",compression = 1)
    var_catch_no <-ncdf4::ncvar_def(name = "catch_no",units = "numbers", dim = listDims, prec = "float",compression = 1)
    # variables <- list(var_lon,var_lat,var_time,var_species,var_gear,var_fleet,var_mode,var_crs,var_main)
    variables <- list(var_crs,var_catch_t,var_catch_no)
  }
  
  
  # missval = fillval,
  nc <- ncdf4::nc_create(ncfilename, vars=variables, force_v4 = TRUE, verbose = FALSE)
  # ncds <-ncdf4::nc_open(nc)
  
  # ncdf4::ncvar_put(nc,varid="time",vals=as.numeric(times))
  # ncdf4::ncvar_put(nc,varid="lon",vals=lons)
  # ncdf4::ncvar_put(nc,varid="lat",vals=lats)
  # ncdf4::ncvar_put(nc,varid="species",vals=species)
  # ncdf4::ncvar_put(nc,varid="gear",vals=gears)
  # ncdf4::ncvar_put(nc,varid="fleet",vals=fleets)
  # ncdf4::ncvar_put(nc,varid="mode",vals=modes)
  ncdf4::ncatt_put(nc,"lon","standard_name","longitude",prec="text")
  ncdf4::ncatt_put(nc,"lon","coverage_content_type","coordinate",prec="text")
  ncdf4::ncatt_put(nc,"lon","axis","X",prec="text")
  ncdf4::ncatt_put(nc,"lon","_CoordinateAxisType","longitude",prec="text")
  # ncdf4::ncatt_put(nc,"lon","valid_min",min(dateVector))
  # ncdf4::ncatt_put(nc,"lon","valid_max",min(dateVector))
  
  ncdf4::ncatt_put(nc,"lat","standard_name","latitude",prec="text")
  ncdf4::ncatt_put(nc,"lat","coverage_content_type","coordinate",prec="text")
  ncdf4::ncatt_put(nc,"lat","axis","Y",prec="text")
  ncdf4::ncatt_put(nc,"lat","_CoordinateAxisType","latitude",prec="text")
  
  ncdf4::ncatt_put(nc,"time","standard_name", "time",prec="text")
  ncdf4::ncatt_put(nc,"time","axis","T",prec="text")
  ncdf4::ncatt_put(nc,"time","_CoordinateAxisType","time",prec="text")
  
  # CRS grid_mapping variable
  ncdf4::ncatt_put(nc,varid="crs","grid_mapping_name","latitude_longitude",prec="text")
  ncdf4::ncatt_put(nc,varid="crs","long_name","CRS definition",prec="text")
  ncdf4::ncatt_put(nc,varid="crs","longitude_of_prime_meridian",0.0)
  ncdf4::ncatt_put(nc,varid="crs","semi_major_axis",6378137.0)
  ncdf4::ncatt_put(nc,varid="crs","inverse_flattening",298.257223563)
  ncdf4::ncatt_put(nc,varid="crs","spatial_ref","GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]")
  ncdf4::ncatt_put(nc,varid="crs","crs_wkt","GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563,AUTHORITY[\"EPSG\",\"7030\"]],AUTHORITY[\"EPSG\",\"6326\"]],PRIMEM[\"Greenwich\",0,AUTHORITY[\"EPSG\",\"8901\"]],UNIT[\"degree\",0.0174532925199433,AUTHORITY[\"EPSG\",\"9122\"]],AXIS[\"Latitude\",NORTH],AXIS[\"Longitude\",EAST],AUTHORITY[\"EPSG\",\"4326\"]]")
  ncdf4::ncatt_put(nc,varid="crs","GeoTransform","-149.5 0.996031746031746 0 48.5 0 -0.9888888888888889 ")
  
  if(metadata$variable == "effort"){
    ncdf4::ncatt_put(nc,varid="effort","grid_mapping","crs",prec="text")
  }else{
    ncdf4::ncatt_put(nc,varid="catch_no","grid_mapping","crs",prec="text")
    ncdf4::ncatt_put(nc,varid="catch_t","grid_mapping","crs",prec="text")
    ncdf4::ncvar_put(nc,varid="catch_t", vals = c(0),start = rep(1,7), count =rep(1,7)) 
  }

  
  
# Define the global attributes as an R list
global_attributes <- list(

  # GDAL_AREA_OR_POINT = "Area",
  # Conventions = "Area",
  # GDAL = "GDAL 3.8.4, released 2024/02/08",
  id = metadata$doi,
  naming_authority = "IRD",
  title = "Global Tuna Atlas Level 2",
  summary = "analagous to an abstract in the paper, describing the data and how they were collected and processed",
  creator_type = "person",
  creator_name = "Julien Barde; Bastien Grasset; Paul Taconet; Taha Imzilen", # Who collected and processed the data up to this point
  creator_email = "julien.barde@ird.fr; bastien.grasset@ird.fr",
  creator_institution = "French Institute for Sustainable Development (IRD, www.ird.fr)",
  creator_url = "https://orcid.org/0000-0002-3519-6141", # OrcID is best practice if possible. Other URLs okay, or leave blank for authors that don't have one.
  history = "This dataset has been produced by merging geo-spatial data of tuna, catches coming from four tuna regional fisheries management organizations: IOTC, ICCAT, IATTC, WCPFC. Some processes have been applied by the French Research institute for sustainable development (IRD) to the raw data: mainly conversion from number of fishes catched to weight of fishes (for catches originally expressed in number), and raisings.",
  acknowledgement = "This work has received funding from the European Union's Horizon 2020 research and innovation programme under the BlueBRIDGE project (Grant agreement No 675680).",
  keywords = "Tuna Fisheries; RFMOs; catch",
  keywords_vocabulary = "CF:NetCDF COARDS Climate and Forecast Standard Names",
  institution = "IRD",
  publisher_name = "Zenodo", # Data centre where your data will be published
  publisher_email = "publisher@email.com",
  publisher_url = metadata$doi,
  license = "https://creativecommons.org/licenses/by/4.0/",
  Conventions = "ACDD-1.3, CF-1.7", # Choose which ever version you will check your file against using a compliance checker
  project = "Globa Tuna Atlas",
  id = metadata$template_filename,
  geospatial_lat_min = as.character(metadata$geospatial_lat_min),
  geospatial_lat_max = as.character(metadata$geospatial_lat_max),
  geospatial_lon_min = as.character(metadata$geospatial_lon_min),
  geospatial_lon_max = as.character(metadata$geospatial_lon_max),
  # geospatial_vertical_min ="0"
  # geospatial_vertical_max ="500"
  # SPATIAL AND TEMPORAL EXTENT
  time_coverage_start = as.character(metadata$time_coverage_start),
  time_coverage_end = as.character(metadata$time_coverage_end)
)

# Loop through the attributes and add them to the NetCDF file. 
for (key in names(global_attributes)) {
  # varid=0 means it is a global attribute
  ncdf4::ncatt_put(nc,varid=0,key,global_attributes[[key]],"text")
  
}
  return(nc)
}

