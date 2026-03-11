write_NetCDF <- function(this_metadata,nb_cores=1){
  
  start_time <- Sys.time()
  print(start_time)
  coverages <- this_metadata$coverages_df$coverages
  variable <- this_metadata$variable
  dir_NetCDF <- this_metadata$dir_NetCDF 
  
  # unregister_dopar <- function() {
  #   env <- foreach:::.foreachGlobals
  #   rm(list=ls(name=env), pos=env)
  # }
  # unregister_dopar()
  # Put Values in one or muliple NetCDF files
  # Set parameters for parallel processing
  # parallel::stopCluster(cl)
  # if(exists("cl")){parallel::stopCluster(cl)
  # # if(class(cl)[1]=="SOCKcluster"){
  # # parallel::stopCluster(cl)
  # }else{
  # nbCores <- parallel::detectCores() - 10
  keptCores <- nb_cores
  # keptCores <- nbCores-2
  # parallel::stopCluster(cl)
  cl <- parallel::makeCluster(keptCores)
  doParallel::registerDoParallel(cl)
  # }
  
  # dataframes to be split into chunks, where the number of chunks is equal to the number of cores on which your doParallel cluster is running.
  # Split the data into chunks
  chunk_size <- nrow(coverages)%/%keptCores
  # data_chunks <- split(coverages, ceiling(seq_along(seq(nrow(coverages))) / chunk_size))
  # data_chunks <- split(coverages, coverages$nb_in_group)
  
  list_groups_assigned <- split_NetCDF(coverages,cores=keptCores)
  coverages <- list_groups_assigned$new_coverages
  data_chunks <- list()
  these_names <- c()
  for(c in 1:keptCores){
    df_item <- coverages %>%  dplyr::filter(group_id %in% list_groups_assigned[[c]]) # %>% arrange(desc(nb_in_group),group_id,group_assigned)
    data_chunks[[length(data_chunks)+1]] = df_item
    these_names <- c(these_names,c)
  }
  names(data_chunks) <- these_names
  list_groups_assigned <- NULL
    
  if(!dir.exists("cube")){
    dir.create(path = "cube")
  }
  
  if(!dir.exists("coverage")){
    dir.create(path = "coverage")
  }
  # Options to split the data frame : for now by unique combinantions of all dimensions (out of spatio-temporal dimensions), 
  # might also be split by species
  # 
  # 
  # for(s in sort(unique(coverages$species_rowid))){
  #   this_dir <-paste0("Species_",s)
  #   if(!dir.exists(this_dir)){
  #     dir.create(path = paste0(file.path(getwd()),"/",this_dir))
  #   }
  # }
  
  template_filename <-this_metadata$template_filename
  file_unit <- this_metadata$file_unit
  lons <- this_metadata$lons
  lats <- this_metadata$lats
  times <- this_metadata$dim_codes$times
  species <- this_metadata$dim_codes$species
  gears <- this_metadata$dim_codes$gears
  fleets <- this_metadata$dim_codes$fleets
  modes <- this_metadata$dim_codes$modes
  
  if(file_unit %in% c("full","both")){
    
    metadata=this_metadata
    metadata$filename=paste0("full_",this_metadata$sp_resolution,"deg.nc")
    metadata$file_unit="coverage_group"
    nc_template <- create_NetCDF_ncdf4(metadata=metadata,
                                       lats=lats$lat,
                                       lons=lons$lon,
                                       times=times$time_day,
                                       species=species$species_rowid,
                                       gears=gears$gear_rowid,
                                       fleets=fleets$fishing_fleet_rowid,
                                       modes=modes$fishing_mode_rowid,
                                       unlim=FALSE)
    ncdf4::nc_close(nc_template)
    
    
  }
  
  if(file_unit %in% c("coverage","both")){
    metadata=this_metadata
    metadata$filename="template_coverage.nc"
    metadata$file_unit="coverage"
    nc_template_coverage <- create_NetCDF_ncdf4(metadata=metadata,
                                                lats=lats$lat,
                                                lons=lons$lon,
                                                times=1,
                                                species=1,
                                                gears=1,
                                                fleets=1,
                                                modes=1
    )
    # print.nc(open.nc("template.nc"))
    # ncdf4::print.ncdf4(ncdf4::nc_open("template_coverage.nc"))
    ncdf4::nc_close(nc_template_coverage)
    
  }
  
  if(file_unit %in% c("coverage_group","both")){
    metadata=this_metadata
    metadata$filename="template_group.nc"
    metadata$file_unit="coverage_group"
    nc_template <- create_NetCDF_ncdf4(metadata=metadata,
                                       lats=lats$lat,
                                       lons=lons$lon,
                                       times=times$time_day,
                                       species=1,
                                       gears=1,
                                       fleets=1,
                                       modes=1,
                                       unlim=FALSE
                                       
    )
    # print.nc(open.nc("template_group.nc"))
    # print(ncdf4::nc_open("template_group.nc"))
    ncdf4::nc_close(nc_template)
  }
  
  start_time <- Sys.time()
  print(start_time)
  print("Start loop")
  
  #LOOP FOR PARALLEL PROCESSING
  # https://r-dev-perf.borishejblum.science/parallelisation-du-code-r
  # https://r-dev-perf.borishejblum.science/rcpp-ou-comment-integrer-facilement-du-code-cdans-un-package-r
  # print(nrow(coverages_chunk)) }
  # coverages_chunk <- data_chunks[[1]]
  # coverages_chunk <- data_chunks$`20`
  # coverages_chunk <- data_chunks$`1`
  foreach(coverages_chunk = data_chunks, .packages = c("sf","raster","dplyr","ncdf4")) %dopar% {
    # list_all_files <- foreach(coverages_chunk = data_chunks, .combine = 'c', .packages = c("sf","raster","dplyr","RNetCDF")) %dopar% {
    # for(s in sort(unique(coverages$species_rowid))){
    
    setwd(dir_NetCDF)
    nb_group_coverages <- unique(coverages_chunk$group_id)
    nb_coverages <- unique(coverages_chunk$coverage_id)
    filename <- NULL
    
    if(file_unit=="coverage_group"  || file_unit=="both"){
      for(g in nb_group_coverages){
        resTDgroup <- coverages_chunk %>% dplyr::filter(group_id==g) 
        nb_coverages <- unique(resTDgroup$coverage_id)
        nb_geo_id <- unique(resTDgroup$geographic_identifier)
        
        if(variable == "catch"){
          filename = paste0(dir_NetCDF,"/cube",template_filename,
                            "_Species_",species[as.numeric(resTDgroup[1,4]),1],
                            "_Gear_",gsub(" ","_",gears[as.numeric(resTDgroup[1,5]),5]),
                            "_Fleet_",fleets[as.numeric(resTDgroup[1,6]),1],
                            "_Mode_",modes[as.numeric(resTDgroup[1,7]),1],
                            "_Group_",resTDgroup[1,17],"_min_Coverage_",min(nb_coverages))
        }else{
          filename = paste0(dir_NetCDF,"/cube",template_filename,
                            "_Gear_",gsub(" ","_",gears[as.numeric(resTDgroup[1,4]),5]),
                            "_Fleet_",fleets[as.numeric(resTDgroup[1,5]),1],
                            "_Mode_",modes[as.numeric(resTDgroup[1,6]),1],
                            "_Group_",resTDgroup[1,16],"_min_Coverage_",min(nb_coverages))
        }
        
        this_nc_t <- NULL
        file.copy(paste0(dir_NetCDF,"/template_group.nc"),to = paste0(filename,".nc"))
        this_nc_t <-ncdf4::nc_open(paste0(filename,".nc"),write=TRUE)
        
        # Prefill the dimensions of length 1
        if(variable == "catch"){
          ncdf4::ncvar_put(this_nc_t,varid="species", vals = as.numeric(resTDgroup[1,4]),start = 1, count =1)
          ncdf4::ncvar_put(this_nc_t,varid="gear", vals = as.numeric(resTDgroup[1,5]),start = 1, count =1)  
          ncdf4::ncvar_put(this_nc_t,varid="fleet", vals = as.numeric(resTDgroup[1,6]),start = 1, count =1) 
          ncdf4::ncvar_put(this_nc_t,varid="mode", vals = as.numeric(resTDgroup[1,7]),start = 1, count =1) 
        }else{
          ncdf4::ncvar_put(this_nc_t,varid="gear", vals = as.numeric(resTDgroup[1,4]),start = 1, count =1)  
          ncdf4::ncvar_put(this_nc_t,varid="fleet", vals = as.numeric(resTDgroup[1,5]),start = 1, count =1) 
          ncdf4::ncvar_put(this_nc_t,varid="mode", vals = as.numeric(resTDgroup[1,5]),start = 1, count =1) 
        }
 
        
        method <- "coverage"
        
        # time series method
        if(method!="coverage"){
          for(gid in nb_geo_id){
            these_time_steps <- empty_time_steps
            resTD <- resTDgroup %>% dplyr::filter(geographic_identifier==gid) # %>% dplyr::select(c("lon","lat","time_rowid","species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_value"))
            # View(resTD)
            for(line in 1:nrow(resTD)){
              these_time_steps[resTD$time_rowid[line]] =resTD$measurement_value[line]
            }
            strt <- as.numeric(resTD[1,2:7])
            # this_data = as.array(as.numeric(these_time_steps))
            this_data = array(as.numeric(these_time_steps), dim = c(length(these_time_steps),rep(1,length(strt))))
            ncdf4::ncvar_put(this_nc_t,varid=paste0("catch_",resTD$measurement_unit[line]), vals = this_data ,start = c(1,strt[1:2],rep(1,length(strt)-2)), count =c(-1, rep(1,length(strt))) )
          }
        }else{
          # ALTERNATIVE: Coverage method
          for(i in nb_coverages){
            resTD <- resTDgroup %>% dplyr::filter(coverage_id==i)
            ##### Check if also aksed to write one NetCDF file per coverage => prepare an empty NetCDF file
            if(file_unit=="coverage"  || file_unit=="both" ){
              
              #To do faire la concatenation du filename dans le dataframe
              if(variable == "catch"){
                coverage_nc_filename = paste0(dir_NetCDF,"/coverage",template_filename,
                                              "_in_",resTD[1,9],
                                              "_Time_",times[as.numeric(resTD[1,1]),4],
                                              "_Species_",species[as.numeric(resTDgroup[1,4]),1],
                                              "_Gear_",gsub(" ","_",gears[as.numeric(resTDgroup[1,5]),5]),
                                              "_Fleet_",fleets[as.numeric(resTDgroup[1,6]),1],
                                              "_Mode_",modes[as.numeric(resTDgroup[1,7]),1],
                                              "_Group_",resTDgroup[1,17],"_Coverage_",as.numeric(resTDgroup[1,12]))
              }else{
                coverage_nc_filename = paste0(dir_NetCDF,"/coverage",template_filename,
                                              "_in_",gsub("\\.","-",resTD[1,8]),
                                              "_Time_",times[as.numeric(resTD[1,1]),4],
                                              "_Gear_",gsub(" ","_",gears[as.numeric(resTDgroup[1,4]),5]),
                                              "_Fleet_",fleets[as.numeric(resTDgroup[1,5]),1],
                                              "_Mode_",modes[as.numeric(resTDgroup[1,6]),1],
                                              "_Group_",resTDgroup[1,16],"_Coverage_",as.numeric(resTDgroup[1,11]))
              }

              
              file.copy(paste0(dir_NetCDF,"/template_coverage.nc"),to = paste0(coverage_nc_filename,".nc"))
              this_coverage_nc <- NULL
              this_coverage_nc <-ncdf4::nc_open(paste0(coverage_nc_filename,".nc"),write=TRUE)
              
              ncdf4::ncvar_put(this_coverage_nc, varid="time",vals = times[as.numeric(resTD[1,1]),5], start = 1, count =1) 
              if(variable == "catch"){
                ncdf4::ncvar_put(this_coverage_nc, varid="species",vals = as.numeric(resTD[1,4]), start = 1, count =1)
                ncdf4::ncvar_put(this_coverage_nc, varid="gear",vals = as.numeric(resTD[1,5]), start = 1, count =1) 
                ncdf4::ncvar_put(this_coverage_nc, varid="fleet",vals = as.numeric(resTD[1,6]), start = 1, count =1) 
                ncdf4::ncvar_put(this_coverage_nc, varid="mode", vals = as.numeric(resTD[1,7]), start = 1, count =1) 
              }else{
                ncdf4::ncvar_put(this_coverage_nc, varid="gear",vals = as.numeric(resTD[1,4]), start = 1, count =1) 
                ncdf4::ncvar_put(this_coverage_nc, varid="fleet",vals = as.numeric(resTD[1,5]), start = 1, count =1) 
                ncdf4::ncvar_put(this_coverage_nc, varid="mode", vals = as.numeric(resTD[1,6]), start = 1, count =1) 
              }

            }
            
            #fill the coverage pixel by pixel (line by line)
            for(line in 1:nrow(resTD)){
              # strt <- as.numeric(resTD[1,])
              # strt <- as.numeric(c(which(lons$lon==as.numeric(resTD[line,1])),
              #                      which(lats$lat==as.numeric(resTD[line,2])),
              #                      # times[as.numeric(resTD[line,3]),3],
              #                      # which(dim_codes$times$time_rowid==as.numeric(resTD[line,3])),
              #                      as.numeric(resTD[line,3:7]))
              #                    )
              strt <- as.numeric(resTD[line,1:7])
              if(variable == "effort"){
                #no species
                strt <- as.numeric(resTD[line,1:6])
              }
              this_data = array(as.numeric(resTD[line,length(strt)+1]), dim = c(rep(1,length(strt))))
              
              if(variable == "effort"){
                #Fill the file for this group of coverages / data cube
                ncdf4::ncvar_put(this_nc_t,varid="effort",vals = this_data,start = c(rep(1,length(strt)-3),strt[1:3]), count =rep(1,length(strt)))
                #Fill the file for this specific coverage
                ncdf4::ncvar_put(this_coverage_nc,varid="effort", vals = this_data,start = c(strt[2:3],rep(1,length(strt)-2)), count =rep(1,length(strt)))
                }else{
                  #Fill the file for this group of coverages / data cube
                  ncdf4::ncvar_put(this_nc_t,varid=paste0("catch_",resTD$measurement_unit[line]), vals = this_data,start = c(rep(1,length(strt)-3),strt[1:3]), count =rep(1,length(strt)))
                  #Fill the file for this specific coverage
                  ncdf4::ncvar_put(this_coverage_nc,varid=paste0("catch_",resTD$measurement_unit[line]), vals = this_data,start = c(strt[2:3],rep(1,length(strt)-2)), count =rep(1,length(strt)))
              }
            }
            ncdf4::nc_close(this_coverage_nc)
            # gdalUtilities::gdal_translate(src_dataset = paste0(coverage_nc_filename,".nc"), dst_dataset = paste0(coverage_nc_filename,".tif"),dryrun = FALSE)
          }
        }
        ncdf4::nc_close(this_nc_t)
      }
    }
  }
  end_time <- Sys.time()
  print(end_time-start_time)
}