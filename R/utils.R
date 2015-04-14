#' store amelia chains
#'
#' Write the iteration data to couchdb for this detector
#'
#' @param df.amelia
#' @param year
#' @param detector.id
#' @param imputation.name
#' @param maxiter default 100
#' @param db the couchdb tracking db, defaults to vdsdata%2ftracking
#' @return itercount, the total count of iterations that hit the "max
#' iteration" limit.  Ideally this will be zero.  If not, the
#' imputation that hit maxiter is likely unusable
#'
store.amelia.chains <- function(df.amelia,year,detector.id,
                                imputation.name='',maxiter=100,
                                db='vdsdata%2ftracking'){
  m <- length(df.amelia$imputations)
  itercount = 0;
  chains=rep(0,m)
  for (i in 1:m) {
    chains[i]=nrow(df.amelia$iterHist[[i]])
    if(chains[i]==maxiter){
      itercount <- itercount + 1
    }
  }
  ## I hate that R does not interpolate list names
  trackerdoc <- list('chain_lengths'=chains,'max_iterations'=itercount)
  if(imputation.name != '' ){
    names(trackerdoc) <- paste(imputation.name,names(trackerdoc),sep='_')
  }
  rcouchutils::couch.set.state(year=year,id=detector.id,doc=trackerdoc,db=db)
  return (itercount)
}


#' Get save path
#'
#' little utility to extract all but the last part of a long file name
#' with the path embedded. Just breaks the incoming path on "/" and
#' then drops the last one (figuring that is the file name) and
#' returns the path preceding the filename part
#'
#' @param f the filename, with path info you want to extract
#' @return the path, not including the filename
#'
get.save.path <- function(f){
    file.names <- strsplit(f,split="/")[[1]]
    savepath <- paste(file.names[-(length(file.names))],collapse='/')
}


#' unzoo incantation
#'
#' This is a utility that does exactly the same thing everytime to
#' revert a dataframe from zoo to a regular dataframe with time info
#' slotted in place
#'
#' @param df.z the zoo'd dataframe for processing
#' @return a dataframe with ts, time of day (tod), and day of week
#' (day) added to it, in addition to the zoo object's core data
unzoo.incantation <- function(df.z){
  ts.ts <- unclass(time(df.z))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
  keep.columns <-  grep( pattern="(^ts|^day|^tod|^obs_count)",x=names(df.z),perl=TRUE,value=TRUE,invert=TRUE)
  df.m <- data.frame(coredata(df.z[,keep.columns]))
  df.m$ts <- ts.ts
  ts.lt <- as.POSIXlt(df.m$ts)
  df.m$tod   <- ts.lt$hour + (ts.lt$min/60)
  df.m$day   <- ts.lt$wday
  df.m
}


#' transpose lanes to rows
#'
#' @param df a dataframe of data with lanes data in there
#' @return an expanded dataframe, with lane data slotted into rows,
#' rather than spread out over multiple columns.
#'
transpose.lanes.to.rows <- function(df){

  varnames <- names(df)
  lane.pattern = '(r\\d+$|l1)$';
  not.lanes <- grep(pattern=lane.pattern,x=varnames,perl=TRUE,value=TRUE,inv=TRUE)
  lanes <- regexec(lane.pattern,varnames)
  lanes <- unique(unlist( regmatches(varnames, lanes) ))
  recode <- data.frame()
  for(lane in lanes){
    lane.columns <- grep(pattern=paste(lane,'$',sep=''),x=varnames,perl=TRUE,value=TRUE)
    if(!length(lane.columns)>0){
      next
    }
    keepvars <- c(not.lanes,lane.columns)
    subset <- data.frame(df[,keepvars])
    names(subset) <- gsub(paste('_?',lane,'$',sep=''),'',keepvars,perl=TRUE)
    subset$lane <- lane

    if(length(recode)==0){
      recode <- subset
    }else{
      ## make sure names match up
      missing.recode <- setdiff(names(recode),names(subset))
      missing.subset <- setdiff(names(subset),names(recode))

      if(length(missing.recode)>0){
        subset[,missing.recode] <- NA
      }
      if(length(missing.subset)>0){
        recode[,missing.subset] <- NA
      }
      recode <- rbind(recode,subset)
    }
  }
  recode
}

#' district from vdsid
#'
#' Simple utility that extracts the Caltrans district number from the
#' VDS id, recognizing that the number that starts the vdsid is the
#' district (1 through 12)
#'
#' @param vdsid
#' @return the district number (1 through 12)
#'
district.from.vdsid <- function(vdsid){
  district <- as.integer(sub( "\\d{5}$","",x=vdsid))
  if(district < 10){
    district <- paste('d0',district,sep='')
  }else{
    district <- paste('d',district,sep='')
  }
  ## setup.district.replication(district,year)
  district
}

#' add time of day
#'
#' add time of day information based on the timestamp in the data frame
#'
#' @param df data frame with a column named "ts"
#' @return dataframe with time of day (tod), day of week (day) and
#' hour of the day (hr) added to it
add.time.of.day <- function(df){
  ## add time of day and day of week here
  ts.lt   <- as.POSIXlt(df$ts)
  df$tod  <- ts.lt$hour + (ts.lt$min/60)
  df$day  <- ts.lt$wday
  df$hr   <- ts.lt$hr
  df
}


#' make amelia output file
#'
#' This will create the proper output file name for an amelia result,
#' given the root path, the file name, as well as the seconds and the
#' year
#'
#' Really this is a stupid function, because I'm already assuming that
#' the year is written into the filename passed in.  this just adds
#' the number of seconds used in the imputation, as well adding on a
#' consistent ending ('.imputed.RData')
#'
#' @param path
#' @param fname
#' @param seconds
#' @param year
#' @return a string representing the full path that you can save this
#' amelia result to
#'
make.amelia.output.file <- function(path,fname,seconds,year){
  paste(path,'/',fname,'.',seconds,'.imputed.RData',sep='')
}

#' make amelia output pattern
#'
#' create a pattern that you can use to search directories for the
#' amelia output file that would have been generated by the above
#' command
#'
#' @param fname
#' @param year
#' @return a string you can pass to the dir command
#'
make.amelia.output.pattern <- function(fname,year){
  paste(fname,'.*',year,'.*imputed.RData',sep='')
}
