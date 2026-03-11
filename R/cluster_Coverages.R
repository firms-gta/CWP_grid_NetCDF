cluster_Coverages <- function(this_df){
  
  coverages_df <- list()
  # this_df <- this_metadata$test_df
  if(this_metadata$variable=="catch"){
    # dimensions <- c("time_rowid","species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_unit")
    #grouping pixels for each coverage = possible combinationof dimensions
    coverages_df$all_coverages <-  this_df  %>% group_by(time_rowid,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit) %>% 
      summarise(nb_pix = n()) %>% ungroup() %>% arrange(species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit,nb_pix) %>% 
      mutate(coverage_id = row_number())
    
    #grouping coverage for all same kind of coverages with different time steps
    coverages_df$nb_coverages_group <-  coverages_df$all_coverages %>% group_by(species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid) %>% 
      summarise(nb_in_group = n(), nb_lines = sum(nb_pix)) %>% ungroup() %>% arrange(species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,nb_in_group) %>% 
      mutate(group_id = row_number(),file_name_suffix=paste(species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,sep="_")) 
    # %>% arrange(desc(nb_in_group))
    
    coverages_df$coverages <- this_df %>% 
      dplyr::left_join(coverages_df$all_coverages, by = c("time_rowid","species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_unit")) %>% #dplyr::select(-c(nb_pix)) %>% 
      dplyr::left_join(coverages_df$nb_coverages_group,by=c("species_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid")) %>% 
      arrange(desc(nb_in_group),group_id,coverage_id,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit,time_rowid)  %>% relocate(time_rowid)
    #number of coverages for this spatial resolution :
    
  }
  if(this_metadata$variable=="effort"){
    # dimensions <- c("time_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_unit")
    #grouping pixels for each coverage = possible combinationof dimensions
    coverages_df$all_coverages <-  this_df  %>% group_by(time_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit) %>% 
      summarise(nb_pix = n()) %>% ungroup() %>% arrange(gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit,nb_pix) %>% 
      mutate(coverage_id = row_number())
    
    #grouping coverage for all same kind of coverages with different time steps
    coverages_df$nb_coverages_group <-  coverages_df$all_coverages %>% group_by(gear_rowid,fishing_fleet_rowid,fishing_mode_rowid) %>% 
      summarise(nb_in_group = n(), nb_lines = sum(nb_pix)) %>% ungroup() %>% arrange(gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,nb_in_group) %>% 
      mutate(group_id = row_number(),file_name_suffix=paste(gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,sep="_")) 
    # %>% arrange(desc(nb_in_group))
    
    coverages_df$coverages <- this_df %>% 
      dplyr::left_join(coverages_df$all_coverages, by = c("time_rowid","gear_rowid","fishing_fleet_rowid","fishing_mode_rowid","measurement_unit")) %>% #dplyr::select(-c(nb_pix)) %>% 
      dplyr::left_join(coverages_df$nb_coverages_group,by=c("gear_rowid","fishing_fleet_rowid","fishing_mode_rowid")) %>% 
      arrange(desc(nb_in_group),group_id,coverage_id,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid,measurement_unit,time_rowid)  %>% relocate(time_rowid)

        #number of coverages for this spatial resolution :
    # length(unique(coverages$coverage_id))
  }
  


  
  # Number of observations for each pixel
  # nb_obs_per_pixel <- coverages %>% group_by(lat,lon,species_rowid,gear_rowid,fishing_fleet_rowid,fishing_mode_rowid) %>% 
    # summarise(nb_time_steps = n()) %>%  arrange(desc(nb_time_steps),lat,lon,species_rowid,gear_rowid,fishing_fleet_rowid)  
  
  # coverages <- coverage %>% arrange(desc(nb_in_group)) 
  
  return(coverages_df) 
}