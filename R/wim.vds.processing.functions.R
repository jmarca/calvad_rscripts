
#' dump tsdf to a database file via write.csv
#'
#' Convenience function that will organize volume, occupancy, and
#' speed (perhaps) data into tidy CSV output.  It will also make sure
#' that the variable names (the header row) are okay with whatever
#' database is stashed in "con"
#'
#' @param vds.id  the vds id
#' @param df the dataframe with time series data
#' @param con the database connection (formerly a global variable!)
#' @param target.file the destination file for the dump op
#' @param ... additional arguments to write.csv
#' @return The result of the call to write.csv
#' @export
dump.tsdf.to.DB.file <- function(vds.id,df,con,target.file,...){
  vds.lanes <- 1
  names.vds <- names(df)
  while(is.element(paste("nr",vds.lanes,sep=''),names.vds)){
    vds.lanes <- vds.lanes+1
  }
  n.idx <- vds.lane.numbers(vds.lanes,c("n"))
  o.idx <- vds.lane.numbers(vds.lanes,c("o"))

  ## create the df suitable for dumping to db
  dump <- data.frame(vds_id=vds.id,ts=df$ts)
  dump$vol <-  apply(df[,n.idx], 1, sum)
  dump$occ <-   apply(df[,o.idx], 1, sum)
  # fixme change this when I get speed
  dump$spd <- NA
  # fixme possible change the sd stuff when/if I read the documentation on handling multiples
  dump$sd_vol <- NA
  dump$sd_occ <- NA
  dump$sd_spd <- NA
  db.legal.names  <- RPostgreSQL::make.db.names(con,names(dump),
                                   unique=TRUE,allow.keywords=FALSE)
  names(dump) <- db.legal.names
  ## fs write
  write.csv(dump,file=target.file,row.names = FALSE,...)
}

#' Make vds wim imputed name
#'
#' Convenience function that will generate a standard name for a
#' vds-wim imputation file.  This is important so that all of the
#' files are named the same way
#'
#' @param wim  the wim site id
#' @param vds  the vds id
#' @param year the year of the data
#' @return a string name
#' @export
make.vds.wim.imputed.name <- function(wim,vds,year){
  paste('wim',wim,'vds',vds,'imputed',year,'RData',sep='.')
}


#' Save imputed combined file
#'
#' An imputed combined file is passed in as amelia output, and saved
#'
#' @param aout Amelia output
#' @param path the root directory to save into
#' @param wim  the wim site id
#' @param vds  the vds id
#' @param year the year of the data
#' @return nothing at all
#' @export
save.imputed.combined <- function(aout,path,wim,vds,year){
  fname <- make.vds.wim.imputed.name(wim,vds,year)
  print(fname)
  result <- try(save(aout,file=paste(path,fname,sep='/'),compress='xz'))
  ## I keep getting errors saving, that bomb out my program.
  if(class(result) == "try-error"){
    print ("\n Error saving file with XZ compression, trying gzip instead \n")
    save(aout,file=paste(path,fname,sep='/'),compress='gzip')
  }
}

#' Insert the detector id as a new column in all data
#'
#'  You are merging two vds sites, and you have to be able to
#' extricate the two after the fact.  So add another id variable for
#' the vdsid
#'
#' @param df the aout data to identify
#' @param id the id value to insert in every record
#' @param colname the column name to give the new id column data.
#' Defaults to 'vds_id'
#' @return the modified dataframe
add.detector.id <- function (df,id,colname='vds_id'){
  if(length(names(df)[names(df)==colname]) == 0){
    df[,colname] <- id
  }
  df
}



#' Check for imputed combined file
#'
#' Will check to see if the imputation is already done, by checking to
#' see if an imputed combined file has already been saved
#'
#' @param path the root directory to save into
#' @param wim  the wim site id
#' @param vds  the vds id
#' @param year the year of the data
#' @return TRUE or FALSE.  TRUE if the file exists, or FALSE if not
#' @export
check.imputed.combined<- function(path,wim,vds,year){
  result = FALSE
  for (vdsi in 1:length(vds)){
    aout <- 'failed'
    fname <- paste('wim',wim,'vds',vds[vdsi],'imputed',year,'RData',sep='.')
    done.file <- dir(path, pattern=fname,
                   full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
    if(length(done.file)>0){
      result = TRUE;
      return(result);
    }
  }
  result
}

#' Load imputed combined file
#'
#' Will load up the Amelia output done previously with wim vds combo.
#'
#' Because it is possible to combine multiple vds detectors here, the
#' code loads up all of the vds files associated with the WIM site.
#'
#' @param path the root directory to save into
#' @param wim  the wim site id
#' @param vds  the vds ids, as a list or vector
#' @param year the year of the data
#' @return the combined results of all the multiple imputations as a dataframe
#' @export
load.imputed.combined <- function(path,wim,vds,year){

  ## vds could be an array, or it might not, collapse handles that
  alldata <- data.frame()
  for (vdsi in 1:length(vds)){
    aout <- 'failed'
    fname <- paste('wim',wim,'vds',vds[vdsi],'imputed',year,'RData',sep='.')
    done.file <- dir(path, pattern=fname,
                   full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
    if(length(done.file)>0){
      print(paste('loading imputed combined file',done.file[1]))
      load.result <-  load(file=done.file[1])

      ## need to merge these here
      aout <- add.detector.id(aout,vds[vdsi],'vds_id')
      problem.vars <- grep( pattern="_all_veh_speed_",x=names(aout),perl=TRUE,value=TRUE)
      ## can't see how to do this all at once
      for(var in problem.vars){
        negative.vals <- aout[,var]<0 & ! is.na(aout[,var])
        aout[negative.vals,var] <- 0
      }
      if(length(alldata)==0){
        alldata <-  aout
      }else{
        alldata <- rbind( alldata, aout )
      }

    }
  }
  alldata
}



#' Load imputed combined file, prefetch version
#'
#' Will load up the Amelia output done previously with wim vds combo.
#'
#' this version creates an array of the directory to check each time.
#' If the directory files change, this isn't appropriate, but
#' otherwise it should be faster than the above because results are
#' precached.
#'
#' See, the thing is that I pull up these imputed combined files a lot
#' for each subsequent impute, so having them handy is fast.  On the
#' other hand, nowadays I invoke a completely new R call for each
#' imputation run, obviating the need and efficiency of this call.  So
#' whatever.  Still faster, as all possible matches are loaded in one
#' pass through the dir command, rather than one pass per file to be
#' loaded.
#'
#' @param path the root directory to save into
#' @param pattern The pattern to use to locate wim vds imputed
#' combined files.  defaults to 'wim.*reduced.*vds.*imputed.*RData'
#' and you probably shouldn't mess with this
#' @param recursive whether or not to recurse down the directory tree
#' from path.  Passed along to the dir argument; defaults to FALSE
#' @return A function that can be called with vds and year to get the
#' appropriate imputed combined file from the precached list of files
#' @export
load.imputed.combined.prefetch <- function(path,pattern='wim.*reduced.*vds.*imputed.*RData',recursive=FALSE) {


  existing.files <- dir(path, pattern=pattern, full.names=TRUE, ignore.case=TRUE, recursive=recursive,all.files=TRUE)

  ## return a function that can be called that searches from this list

  #' A closure function that will use the prefeched directory reading results
  #' @param vds  the vds ids, as a list or vector
  #' @param year the year of the data
  #' @return the combined results of all the multiple imputations as a dataframe
  callback <- function(vds,year){

    ## vds could be an array, or it might not, collapse handles that
    alldata <- list()
    for (vdsi in 1:length(vds)){
      aout <- 'failed'
      vds.pattern <- paste( vds[vdsi],'.*',year,sep='')
      fname <- grep(vds.pattern,existing.files,perl=TRUE,value=TRUE)
      if(length(fname)>0){
        for(file in fname){
          print(paste('loading imputed combined file',file))
          load.result <-  load(file=file)
          if(!is.null(aout)){
            ## need to merge these here
            aout <- add.detector.id(aout,vds[vdsi],'vds_id')
            problem.vars <- grep( pattern="_all_veh_speed_",x=names(aout),perl=TRUE,value=TRUE)
            ## can't see how to do this all at once
            for(var in problem.vars){
              negative.vals <- aout[,var]<0 & ! is.na(aout[,var])
              aout[negative.vals,var] <- 0
            }
            alldata[[file]] <- aout
          }
        }
      }
    }
    alldata
  }
  return (callback)
}
