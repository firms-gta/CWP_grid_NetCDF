split_NetCDF <- function(this_df,cores){
  
  #initialize the dataframe to be split 
  these_coverages <- this_df %>% mutate(group_assigned = group_id)
  # these_coverages <- this_metadata$coverages_df$coverages %>% mutate(group_assigned = NA) %>% arrange(group_id,coverage_id) 
  # cores <- 20
  #initialize the list storing the vector will the name of group if for each core 
  list_groups_assigned <- list()
  
  
  #####################split by group
  # all_groups <- sort(unique(these_coverages$group_id)) %>% as.data.frame() %>%  setNames(c("group_id")) %>% mutate(id = row_number())
  # cpl <- these_coverages %>%  dplyr::group_by(id = consecutive_id(nb_in_group, group_id),nb_in_group,group_id) %>% summarise()  %>% arrange(group_id,nb_in_group)
  cpl <- these_coverages %>%  dplyr::group_by(group_id,nb_in_group,nb_lines) %>% summarise()  %>% 
    arrange(desc(nb_lines),group_id)
  # cpl <- these_coverages %>%  dplyr::group_by(nb_in_group,group_id) %>% summarise()  %>% arrange(nb_in_group,group_id) 
  
  # checking
  # sort(unique(cpl$group_id))
  # sum(cpl$nb_in_group)
  # sum(cpl$nb_lines)
  # nrow(these_coverages)
  # sort(unique(these_coverages$group_id))
  # View(these_coverages)
  
  # vector storing all groups ids
  # total_gps <- cpl$group_id
  # sort(unique(cpl$group_id))
  # total_gps <- cpl$nb_in_group

  # define the number of lines for each core
  max_nb_lines_per_gp <- nrow(these_coverages) %/% cores
  # 110338
  
  # set variables
  total_assigned <- 0
  left_behind_total <-0
  names_list <- c()
  groups_non_assigned <- c()
  this_group_assigned <- c()
  # cluster_unit <- "nb_in_group"
  # cluster_unit <- "nb_lines"
  
  overhead <- cpl  %>%  dplyr::filter(nb_lines >= max_nb_lines_per_gp) 
  # for of lines superior to the max_nb_lines_per_gp we directly allocate them to one core
  cpl$remaining_lines <- 0
  cpl$new_assigned_lines <- 0
  cpl$difference_calculation <- 0
  this_iteration <- 0
  offset=0
  
  if(nrow(overhead)>0){
    for(l in 1:nrow(overhead)){
      # round(cpl$nb_lines[n]/max_nb_lines_per_gp)
      sub_groups <- overhead$nb_lines[l] %/% max_nb_lines_per_gp
      print(l)
      print(overhead$nb_lines[l])
      offset = 0
      this_gp_number_lines <- c()
      new_begin <- 1+sum(overhead$nb_lines[1:l])-overhead$nb_lines[l]
      
      for(s in 1:sub_groups){
        this_iteration = this_iteration + 1
        print(paste0("group",s))
        # start <- (s-1)*max_nb_lines_per_gp+1+(sum(overhead$nb_lines[1:l])-overhead$nb_lines[l])+offset
        start <- new_begin
        # end <- s*max_nb_lines_per_gp+(sum(overhead$nb_lines[1:l])-overhead$nb_lines[l])+offset
        end <- start + max_nb_lines_per_gp-1
        last_coverage <- these_coverages$coverage_id[end]
        real_end <- end
        while(these_coverages$coverage_id[real_end+1]==these_coverages$coverage_id[real_end]){
          real_end <- real_end+1
        }
        new_begin <- real_end+1
        this_gp_number_lines <- c(this_gp_number_lines,real_end-start+1)
        offset <- real_end-end
        print(paste0("Iteration",this_iteration))
        print(paste0("start",start))
        print(paste0("end",end))
        print(paste0("real_end",real_end))
        print(paste0("Nb in the group",real_end-start+1))
        print(paste0("offset",offset))
        
        these_coverages$group_id[start:real_end] <- paste0(overhead$group_id[l],"_",s)
        these_coverages$group_assigned[start:real_end] <- paste0(overhead$group_id[l],"_",s)
        list_groups_assigned[[this_iteration]]  <- c(paste0(overhead$group_id[l],"_",s))
        cores <- cores-1
        
        # groups_name <- paste0("vector_",n+this_iteration)
        # names_list <- c(names_list,groups_name)
        # names(list_groups_assigned) <- names_list
        # 
      }
      # cpl$remaining_lines[l] <- cpl$nb_lines[l] - (real_end-(sum(overhead$nb_lines[1:l])-overhead$nb_lines[l]))
      cpl$remaining_lines[l] <- sum(overhead$nb_lines[1:l]) - real_end
      cpl$new_assigned_lines[l] <- sum(this_gp_number_lines)
    }
  }
  
  cpl$difference_calculation <- cpl$nb_lines - cpl$new_assigned_lines
  # View(these_coverages)
  
  cpl <- cpl %>%  dplyr::select(group_id,nb_in_group,difference_calculation) %>% 
    dplyr::rename(nb_lines=difference_calculation) %>% arrange(desc(nb_lines))
  
  # View(these_coverages)
  # these_coverages %>%  mutate(group_assigned = if_else(is.na(group_assigned), NA, time))
  # these_coverages_new <-these_coverages %>%  mutate(group_assigned = case_when(is.na(group_assigned) ~ as.character(group_id),
  #                                                        TRUE ~ group_assigned
  #                                                        ))
  # Round number of iteration for each core to cover the whole dataframe (minus remaining lines from rounding)
  iteration <- nrow(cpl)%/%cores
  # Number of remaining lines from rounding
  remaining_lines <- nrow(cpl) - iteration*cores
  
  # Loop to split dataframe : one chunck per Core
  for(n in 1:cores){
    index_core <- n
    # index_core <- 1+cores-n
    this_total_name <- paste0("total_",n)
    this_total <-NULL
    this_group_assigned <-NULL
    this_total <-cpl$nb_lines[index_core]
    this_group_assigned <- c(this_group_assigned,cpl$group_id[index_core])
    # groups_name <- paste0("vector_",n+this_iteration)
    # names_list <- c(names_list,groups_name)
    print(this_total_name)
    print(this_total)
    left_behind <-0
    # print(this_total)
    # Check i the number of lines in the current group is superior to the max_nb_lines_per_gp, if yes
    for(i in 2:iteration){
        # index <- cores*i-(n-1)
        # print(paste0("ITERATION NB",n))
        # print(paste0("ITERATION",index))
        if((this_total + cpl$nb_lines[(cores*i-(n-1))]) < max_nb_lines_per_gp){
          this_total <- this_total + cpl$nb_lines[(cores*i-(n-1))]
          this_group_assigned <- c(this_group_assigned,cpl$group_id[(cores*i-(n-1))])
        }else{
          # print(paste0("warning:", cpl$nb_lines[(cores*i-(n-1))], " with current total:", this_total))
          left_behind  <- left_behind + cpl$nb_lines[(cores*i-(n-1))]
          groups_non_assigned <-c(groups_non_assigned,cpl$group_id[(cores*i-(n-1))])
          # list
        }
      }
      print(paste0("Temps intermédiaire : ",this_total, ", i=",i))
      # print(paste0("Non attribués: ",groups_non_assigned))
      #if still under the expected number
      if((this_total + cpl$nb_lines[(cores*i-(n-1))]) < max_nb_lines_per_gp){
        print(paste0("On en rajoute:","pour la route",this_total))
        while(this_total <= max_nb_lines_per_gp && length(groups_non_assigned) > 0){
          #how much is left to reach the expected number
          remaining_nb <-  max_nb_lines_per_gp - this_total
          #select remaining / non assign lines
          remaining <- cpl %>%  dplyr::filter(group_id %in% groups_non_assigned) %>% arrange(desc(nb_lines),group_id) 
          #Add the closest value from remaining number in previous lines
          this_total <- this_total + remaining$nb_lines[which.min(abs(remaining$nb_lines-remaining_nb))]
          #Add and remove this group_id from non assigned to assigned
          this_group_assigned <- c(this_group_assigned,remaining$group_id[which.min(abs(remaining$nb_lines-remaining_nb))])
          groups_non_assigned <- groups_non_assigned[!groups_non_assigned == remaining$group_id[which.min(abs(remaining$nb_lines-remaining_nb))]]
          # left_behind  <- left_behind - cpl$nb_lines[remaining$group_id[which.min(abs(remaining$nb_lines-remaining_nb))]]
        }
      }
    
    # print("###############################################################")
    print(paste0("###################### Résultat final for ", this_total_name," = ", this_total, ", i=",i))
    list_groups_assigned[[n+this_iteration]]  <- sort(this_group_assigned)
    print(this_total)
    print(paste0("left_behind :",left_behind))
    total_assigned <-total_assigned + this_total
    left_behind_total <- left_behind_total + left_behind
    }
  
  if(remaining_lines > 0){
    for(r in 1:remaining_lines){
      index <- iteration*cores + r 
      this_total <- this_total + cpl$nb_lines[index]
      this_group_assigned <- c(this_group_assigned,cpl$group_id[index])
      this_group_assigned <- sort(this_group_assigned)
      list_groups_assigned[[n]]  <- sort(this_group_assigned)
    }
  }

  
  # names(list_groups_assigned) <- names_list
  print("###############################################################")
  print(paste0("######### groups_non_assigned ", groups_non_assigned))
  print(paste0("total_assigned :",total_assigned))
  print(paste0("left_behind_total :",left_behind_total))
  print(paste0("left_behind_total :",left_behind_total))
  print("###############################################################")
  # total_assigned + left_behind_total
  # sum(cpl$nb_lines)
  
  # Checking...
  # total_v <- c()
  # for (v in names(list_groups_assigned)){
  #   # print(v)
  #   total_v <- c(total_v,list_groups_assigned[[v]])
  #   }
  # length(total_v)
  # total_v <-sort(unique(total_v))
  # setdiff(as.character(unique(cpl$group_id)),as.character(total_v))
  # remaining <- cpl %>%  dplyr::filter(group_id %in% groups_non_assigned) %>% arrange(desc(nb_in_group),group_id) 
  list_groups_assigned$new_coverages <- these_coverages
 return(list_groups_assigned) 
}