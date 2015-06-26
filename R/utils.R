#' store amelia chains
#'
#' Write the iteration data to couchdb for this detector
#'
#' @param df.amelia the amelia output
#' @param year the year
#' @param detector.id the detector id
#' @param imputation.name which imputation is this? self, raw, truck,
#'     whatever.  will be used in the couchdb stored row
#' @param maxiter default 100
#' @param db the couchdb tracking db, defaults to vdsdata\%2ftracking
#' @return itercount, the total count of iterations that hit the
#'     "max iteration" limit.  Ideally this will be zero.  If not, the
#'     imputation that hit maxiter is likely unusable
#' @export
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
  df.m <- data.frame(zoo::coredata(df.z[,keep.columns]))
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
#' @export
transpose.lanes.to.rows <- function(df){

  varnames <- names(df)
  lane.pattern = '(r\\d+$|l1)$';
  not.lanes <- grep(pattern=lane.pattern,x=varnames,perl=TRUE,value=TRUE,invert=TRUE)
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
##' extract unique lanes
##'
##' @title extract_unique_lanes
##' @param varnames a vector of variable names to sift through.  If
##'     you're lazy, you can pass in a data frame as the second
##'     argument, and I'll call names on it to get varnames.  The
##'     advantage of passing varnames is maybe you don't want to pass
##'     around copies of the full dataframe.
##' @param df a data frame with lane names like nl1, etc
##' @return a list
##' @author James E. Marca
##' @export
##'
extract_unique_lanes <- function(varnames=NULL,df=NULL){
    if(missing(varnames)){
        varnames <- names(df)
    }
    if(is.data.frame(varnames)){
        varnames <- names(varnames)
    }
    keepvars <- grep('[r|l]\\d+$',x=varnames,perl=TRUE,value=TRUE)
    res <- regexpr("([rl]\\d+)",keepvars,perl=TRUE)
    c(unique(do.call(rbind, lapply(seq_along(res), function(i) {
        if(res[i] == -1) return("")
        stt <- attr(res, "capture.start")[i, ]
        stp <- stt + attr(res, "capture.length")[i, ] - 1
        substring(keepvars[i],stt,stp)
    }))))

}

#' district from vdsid
#'
#' Simple utility that extracts the Caltrans district number from the
#' VDS id, recognizing that the number that starts the vdsid is the
#' district (1 through 12)
#'
#' @param vdsid the detector id
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
#' @param path the path
#' @param fname the base file name
#' @param seconds the seconds used in aggregating prior to the amelia run
#' @param year the year
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
#' @param fname the core file name
#' @param year the year
#' @return a string you can pass to the dir command
#' @export
make.amelia.output.pattern <- function(fname,year){
  paste(fname,'.*imputed.RData',sep='')
}

##' extract the detector id, year, from a filename
##'
##' This function will inspect the passed in filepath to extract the
##' year and site information.  If it is a WIM output file, it will
##' return site_no, direction and year.  If it is a VDS site, it will
##' return the vds_id, year.
##' @title decode_amelia_output_file
##' @param filepath the full filename.  the path is required for
##'     proper WIM file decoding
##' @return a named vector.  A WIM file will have the names 'site_no,
##'     direction, year', and a VDS file will have the names 'vds_id,
##'     year'
##'
##' @author James E. Marca
##' @export
##'
decode_amelia_output_file <- function(filepath){
    ## for vds, pattern is vdsid, "ML" year, seconds, imputed.RData
    ## for wim, the year is buried in the path, as in
    ## 2012/37/S/wim37S.3600.imputed.RData
    haswim <- grep(pattern='wim',x=filepath,perl=TRUE,ignore.case=TRUE)
    result <- list()

    if(length(haswim)>0){
        ## a wim site, parse accordingly
        wimpattern <- '(?<year>\\d{4})\\/(?<site_no>\\d+)\\/(?<direction>[[:alpha:]])\\/.*imputed.RData'

        captures <- regexpr(pattern=wimpattern,text=filepath,perl=TRUE)
        starts <- attr(captures,'capture.start')
        ends <- starts + attr(captures,'capture.length')-1
        result <- data.frame(t(substring (text=filepath,first=starts,last=ends)),
                             stringsAsFactors = FALSE)
        names(result) <- attr(captures,'capture.names')
        result[,c('site_no','year')] <- as.numeric(result[,c('site_no','year')])
    }else{
        ## a vds site
        ## example: 1211682_ML_2012.120.imputed.RData
        ## all the info is in the filename part. no need to use path
        vdspattern <- '(?<vds_id>\\d+)_ML_(?<year>\\d{4}).*imputed.RData'
        captures <- regexpr(pattern=vdspattern,text=filepath,perl=TRUE)
        starts <- attr(captures,'capture.start')
        ends <- starts + attr(captures,'capture.length')-1
        result <- data.frame(t(substring (text=filepath,first=starts,last=ends)),
                             stringsAsFactors = FALSE)
        names(result) <- attr(captures,'capture.names')
        result[,c('vds_id','year')] <- as.numeric(result[,c('vds_id','year')])
    }
    result
}
##' Detect an offset between imputed data and original data
##'
##' A few imputations were performed with a broken time offset (I hate
##' timezones and daylight savings time!).  This routine will use the
##' passed in fname, year, and path to find the paired imputation and
##' raw data, and then inspect the timestamp for the first
##' **non-imputed** data entry.  It should match the raw data.
##'
##' @title detect broken imputed time
##' @param fname the usual pattern for finding files.  typically the
##'     detectorid_ML_year, but maybe something else.  Will be passed
##'     to the standard file finding routines along with the year and
##'     the path
##' @param year the year
##' @param path the root path to the data.  Used in the find command
##'     (dir) so the closest to actual, the faster the search.
##' @param delete_it if set to TRUE, then if this routine detects an
##'     offset in the time, the offset imputed file will be deleted
##' @param trackingdb the couchdb database for saving state
##' @return TRUE if the times are offset, FALSE if not
##' @author James E. Marca
##'
detect_broken_imputed_time <- function(fname,year,path,delete_it=FALSE,trackingdb){
    vds.id <-  get.vdsid.from.filename(fname)
    input_data <- load.file(fname=fname,year=year,path=path)
    if(dim(input_data)[1] == 0){
        return (TRUE)
    }
    amelia_data <- get.amelia.vds.file.local(vdsid=vds.id,year=year,path=path)
    impute1 <- amelia_data$imputations[[1]]

    ## use time diff in amelia_data to determine the number of seconds
    ## in the aggregation performed
    seconds <- as.numeric(difftime(time1=impute1$ts[2],
                                   time2=impute1$ts[1],
                                   units='secs'))
    ## prep input data as if for imputation
    lanes <- longway.guess.lanes(input_data)
    df.vds.agg <- vds.aggregate(input_data,input_data$ts,lanes,seconds)

    ## at this point, the time *should* match up
    print('input data')
    print(head(df.vds.agg))
    print('imputed output')
    print(head(impute1))

    ## find the first non-imputed record
    idx <- 1
    no_impute_needed <- seconds/30
    while(idx < length(impute1$ts) &&
          impute1$obs_count[idx] != no_impute_needed){
              idx <- idx+1
          }

    ## so the first non-imputed data point is at idx now
    result <- impute1$ts[idx] != df.vds.agg$ts[idx]

    if(delete_it && result){
        ## mismatch, delete the file
        ## copy and paste code block from get.amelia.vds.file.local
        target.file <- make.amelia.output.pattern(vds.id,year)
        isa.df <- dir(path, pattern=target.file,
                      full.names=TRUE,
                      ignore.case=TRUE,
                      recursive=TRUE)
        ## keep the file with the correct year
        right_file <- grep(pattern=year,x=isa.df,value=TRUE)
        unlink(right_file)
    }
    return (result)
}
