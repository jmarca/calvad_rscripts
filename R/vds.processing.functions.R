
#' sanity check
#'
#' make sure the data is worth processing
#'
#' This is a collection of checks that weed out most (but not all) of
#' the reasons why Amelia will stop working.  I try to be
#' conservative.  For example, I check only that the average count be
#' at least 0.0001, which is pretty small.  But I'd rather pass along
#' here and crash in Amelia than stop out a file that can actually be
#' used.
#'
#' @param data the dataframe containing the data
#' @param ts the timestamp sequence for each record in data
#' @param year the year of the data
#' @param vdsid the detector's id
#' @param db the couchdb to save trcking info.  defaults to
#' "vdsdata\%2ftracking"
#' @return TRUE if the data is good to go, FALSE if not
#'
#' Side effect, will store the reason for data rejection to CouchDB,
#' and also will dump test results to the log file
sanity.check <- function(data,ts,year=0,vdsid='missing',db='vdsdata%2ftracking'){
    problem <- list()
    print ('check dimensions')
  return.val <- dim(data)[2] > 0  ## catch empty data right away

    if(!return.val){
        problem['rawdata'] <- 'no rows of data in raw vds file'
    }
    if(return.val){
        print ('check exists right hand lane data')
        return.val <- is.element("nr1",names(data))    ## sometimes get random interior lanes
        if(!return.val){
            problem['rawdata'] <- 'have data, but not right hand lane? in raw vds file'
        }
    }

    if(return.val){
        names.vds <- names(data)
        max.lanes <- 8
        lane <- 0

        print ('check n and o are always paired')

        ## first check that both n and o are always there together
        while( return.val & lane < max.lanes ){
            lane <- lane + 1
            if( ( is.element(paste("nr",lane), names.vds ) & ! is.element(paste("or",lane), names.vds ) ) |
               ( is.element(paste("or",lane), names.vds ) & ! is.element(paste("nr",lane), names.vds ) ) ){
                problem['rawdata'] <- paste('do not have both occupancy and counts for lane',paste("nr",lane),'in raw vds file')
                return.val <- FALSE
            }
            if( ( is.element(paste("nl",lane), names.vds ) & ! is.element(paste("ol",lane), names.vds ) ) |
               ( is.element(paste("ol",lane), names.vds ) & ! is.element(paste("nl",lane), names.vds ) ) ){
                problem['rawdata'] <- paste('do not have both occupancy and counts for lane',paste("nl",lane),'in raw vds file')
                return.val <- FALSE
            }

        }
    }
    if(return.val){
        ## now check if there *is* a speed value, that there is an n and o value

        lane <- 0
        while( return.val & lane < max.lanes ){
            lane <- lane + 1
            if( is.element(paste("sr",lane), names.vds ) & (! is.element(paste("nr",lane), names.vds ) & ! is.element(paste("or",lane), names.vds ) ) ){
                problem['rawdata'] <- paste('have a speed value, but not both occupancy and counts for lane',paste("nr",lane),'in raw vds file')
                print(problem['rawdata'])
                return.val <- FALSE
            }
            if( is.element(paste("sl",lane), names.vds ) & (! is.element(paste("nl",lane), names.vds ) & ! is.element(paste("ol",lane), names.vds ) ) ){
                problem['rawdata'] <- paste('have a speed value, but not both occupancy and counts for lane',paste("nr",lane),'in raw vds file')
                print(problem['rawdata'])
                return.val <- FALSE
            }
        }
    }
    if(return.val){
        ## can't do any imputation unless you have at least a month of data
        print('check for 4 weeks between first obs, last obs')
        difference <- difftime(ts[length(ts)],ts[1],units='weeks')
        if(difference < 4){
            problem['rawdata'] <- paste('need more than 4 weeks of data, have only',difference,'weeks','in raw vds file')
            return.val <- FALSE
        }
    }
    if(return.val){ ## still going good, do some more checks
        lanes <- longway.guess.lanes(data)
        n.idx <- vds.lane.numbers(lanes,c("n"))
        print('check that if there is a left lane, that there is volume data')
        if(lanes > 1 && ! length(data$nl1) > 0){
            problem['rawdata'] <- paste('do not have counts in left lane','in raw vds file')
            return.val <- FALSE
        } else {
            print('check that if there is a left lane, that there is occupancy data')
            if(lanes>1 && ! length(data$ol1) > 0 ){
                problem['rawdata'] <- paste('do not have occupancies in left lane','in raw vds file')
                return.val <- FALSE
            } else {
                print('check that mean volumnes are sufficiently above zero in all lanes')
                bad.lanes <- 'okay'
                for(i in 1:length(n.idx)){
                    print(paste(n.idx[i]))
                    mean.bug <- mean(data[,n.idx[i]],na.rm=TRUE)
                    print(mean.bug)
                    if(mean.bug < 0.0001){
                        bad.lanes = n.idx[i]
                        break
                    }
                }

                if(bad.lanes != 'okay'){
                    print ('problm')
                    problem['rawdata'] <- paste('mean volumes too low in lane:',bad.lanes,'in raw vds file')
                    return.val <- FALSE
                }
            }
        }
    }
    if(return.val){ ## check that we're not stuck on zero
        ## possible bug
        ## return.val <- max(data$nl1,na.rm=TRUE)>0
        print('check max count value >0 ')
        return.val <- max(data[,n.idx],na.rm=TRUE)>0

        if(!return.val){
            problem['rawdata'] <- paste('max count is zero','in raw vds file')
        }
    }
    if(return.val){ ## check for 4 weeks of raw data
        print('check that have at least 4 weeks of non-null observations')
        return.val <- length(data[!is.na(data[,n.idx[1]]),1]) > 2*60*24*7*4 # 4 weeks * 2 obs/min * 60 min/hr * 24 hr/day * 7 days/week
        if(!return.val){
            weeks.data <- length(data[!is.na(data[1]),1]) / 2*60*24*7
            problem['rawdata'] <- paste('need at least 4 weeks of raw data total.  Have only',weeks.data,'in raw vds file')
        }
    }
    if(!return.val){
        ## save to couchdb
        print('set state  not okay in couchdb')
        if(year != 0 & vdsid != 'missing' ){
            rcouchutils::couch.set.state(year,vdsid,doc=problem,db=db)
        }

    }
    print('done with sanity check')
    return.val
}

#' longway guess lanes
#'
#' guess the number of lanes, based on the data in hand
#'
#' On *could* look up the number of lanes from the metadata, but that
#' is dumb because it is often wrong and we have the data right here.
#' Just look at each record to see how many lanes of data there are.
#'
#' @param data the data for the year
#' @return the number of lanes for this VDS site
#' @export
longway.guess.lanes <- function(data){
    vds.lanes <- 1
    nms <-  names(data);
    names.vds <- grepl( pattern="(^n\\d|^nr\\d|^nl\\d)",x=nms,perl=TRUE)
    names.vds <- nms[names.vds]
    while(is.element(paste("nr",vds.lanes,sep=''),names.vds) | is.element(paste("n",vds.lanes,sep=''),names.vds)){
      vds.lanes <- vds.lanes+1
    }
    if(! is.element("nl1",names.vds)){
      vds.lanes <- vds.lanes-1
    }
    vds.lanes
}

#' recode lanes
#'
#' This will rename the lanes in my special way, that is better for
#' imputing missing values
#'
#' recode to be right lane (r1), right lane but one (r2), r3, ... and then
#' left lane (l1)
#'
#' call ONLY after calling trim empty lanes
#'
#' @param df the data
#' @return new names for the lanes, based on the above logic.
#'
#' Will rename speed (s), count (n) and occupancy (o) columns
#'
recode.lanes <- function(df){
                                        # run this only after you've
                                        # run trim empty lanes

  ##
  ##
  lanes <- longway.guess.lanes(df)
  names.vds <- names(df)

  raw.data <- c('n','o')
  if(is.element("s1",names.vds)){
    raw.data <- c('n','o','s')
  }

  print(paste('in recode lanes',lanes))
  print(paste(raw.data))


  Y <- vds.lane.numbers(lanes,raw.data)
  names(df) <- Y
  df
}


#' verify imputation was okay
#'
#' deprecated, I guess.  Unused any any event
#'
#' @param fname the business end of the file
#' @param path the path
#' @param year the year
#' @param seconds the number of seconds to aggregate
#' @param df.vds.agg.imputed the data frame of aggregated data
#' @return TRUE if okay, FALSE if not okay
verify.imputation.was.okay <- function(fname,path,year,seconds,df.vds.agg.imputed=NA){
  amelia.dump.file <- make.amelia.output.pattern(fname,year)
  done.file <- dir(path, pattern=amelia.dump.file,
                   full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
  load.result <-  load(file=done.file[1])
  okay <- TRUE
  if(load.result!='reject'){
    ## paradoxically, if load result == reject, then no imputation, so return that it is okay!

    ## check that all of the missingness is the same for volume and occupancy
    var.count <- length(df.vds.agg.imputed$missMatrix[1,])
    ## less one at the front for ts, and 3 at the back for other values
    missingcount <- length(df.vds.agg.imputed$missMatrix[1,])
    miss.pattern <- df.vds.agg.imputed$missMatrix[,2]
    correct.count <-  sum( miss.pattern == miss.pattern )  ## hack.  sum of TRUE/FALSE == count of TRUEs
    for (i in 3:(missingcount-3)){
      okay <-  okay & (miss.pattern == df.vds.agg.imputed$missMatrix[,i])
    }
  }
  okay
}


#' Make a file name for the DB dump routine
#'
#' Just push together the strings in a consistent way.
#'
#' @param path where you want to store the data
#' @param vds.id the ID of the VDS site, or really whatever site
#' you're talking about
#' @param year the year
#' @return a canonical filename for the DB dump
make.db.dump.output.file <- function(path,vds.id,year){
  paste(path,paste('vds_hour_agg',vds.id,year,'dat',sep='.'),sep='/')
}

#' parse the passed in filename and extract the VDS id from it
#'
#' A bit of a hack.  Don't expect it to be super smart.  It isn't and
#' will break on strange input
#'
#' @param filename the filename to process
#' @return the VDS id Really, pass this well formed input, because I
#' don't even bother to make sure that the vdsid is all numbers. I
#' just look for [vdsid]_[vdstype]_[year], split on underscores, and
#' return the first value
#' @export
get.vdsid.from.filename <- function(filename){
  ## files format is [vdsid]_[vdstype]_[year]
  vds.id <-  strsplit(filename,"_")[[1]][1]
  vds.id
}





#' hourly aggregate VDS site data for a year
#'
#' pretty much unused, but it will read in down to raw CSV data, and
#' will dump out hourly data as a csv file
#'
#' @param fname the file name.  Just the name
#' @param f the full path to the data, above
#' @param path path to the files yes, this is lame but it is old
#' unused and not worth refactoring at the moment
#' @param year the year
#' @param vds.id the id of the VDS detector
#' @param con database connection to determine legal db column names
#' @return whatever
#'
#' sideeffect is to save an hourly CSV file to the right path
#'
hourly.agg.VDS.site <- function(fname,f,path,year,vds.id,con){
  ## aggregate non-missing data
  ## return 1 if properly aggregated, return 0 if not
  returnval <- 0
  seconds <- 3600 ## hourly!

  ## fname is the filename for the vds data.
  ## f is the full path to the file I need to grab

  ## is there a df available?
  ts <- data.frame()
  df <- data.frame()
  target.file =paste(fname,'.df.*',year,'RData',sep='')
  isa.df <- dir(path, pattern=target.file,full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
  need.to.save <- FALSE
  if(length(isa.df)>0){
    print (paste('loading', isa.df[1]))
    load.result <-  load(file=isa.df[1])
    ## break out ts
    ts <- df$ts
    df$ts <- NULL
  }else{
    print (paste('scanning', f))
    fileScan <- load.raw.file(f)

    ## pre-process the vds data
    ts <- as.POSIXct(strptime(fileScan$ts,"%m/%d/%Y %H:%M:%S",tz='GMT'))
    df <- trim.empty.lanes(fileScan)
    if(dim(df)[2]>0                    ## sometimes df is totally NA
       & is.element("n1",names(df))    ## sometimes get random interior lanes
       ){
       df <- recode.lanes(df)
    }
    ## save for next time, if needed
    df$ts <- ts
    save(df,file=paste(path,'/',fname,'.df.',year,'RData',sep=''),compress='xz')
    df$ts <- NULL
  }

  lanes <- longway.guess.lanes(df)
  df.vds.agg <- data.frame()
  good.periods <- c(FALSE)
  if(lanes>0 && difftime(ts[length(ts)],ts[1],units='hours') > 1){
    df.vds.agg <- vds.aggregate(df,ts,lanes=lanes,seconds=seconds)

    good.periods <- df.vds.agg$obs_count==seconds/30   & ! is.na(df.vds.agg$obs_count)
    ## will also use that later to save "events" of good data, imputed data

    ## if good.periods is all falsy, then, like, you know, the data is bad
  }
  other <- data.frame()
  if(length(good.periods[good.periods])==0){
    ## save something and be done
    ## but it could be that even df.vds.agg is blank.
    if(length(df.vds.agg)>0){
      other <- df.vds.agg[1,]
      other$vds_id <- vds.id
    }else{
      ts1.year.lt <- as.POSIXlt(ts)
      other <- data.frame(ts=ts[1],
                          tod= ts1.year.lt$hour[1] + ts1.year.lt$min[1]/60,
                          day=ts1.year.lt$wday[1])
      other$vds_id <- vds.id
      other$nr1 <- NA
      other$or1 <- NA

    }
  }else{
    other <- df.vds.agg[good.periods,]
    other$vds_id <- vds.id
  }
  ## need to break up the lane data into rows from columns
  other <- transpose.lanes.to.rows (other) ## should work, unless agg is wierd
  db.legal.names  <- RPostgreSQL::make.db.names(con,names(other),unique=TRUE,allow.keywords=FALSE)
  names(other) <- db.legal.names
  filename <- paste(path,'/',fname,'.',year,'rawaggregate.csv',sep='')
  write.csv(other,file=filename,row.names = FALSE)
  rm(other)

  events <- summarize.events(df.vds.agg,year,good.periods,vds.id,ts)
  save.events.file(path,fname,year,events)
}

#' save events file
#'
#' write out the events dataframe to a file using CSV
#'
#' @param path the path where you want to stick fname
#' @param fname the file name to save data to
#' @param year the year of data
#' @param events the dataframe containing the events data
#' @return the output of call to \code{\link{save.events.file}}
save.events.file <- function(path,fname,year,events){
  filename <- paste(path,'/',fname,'.',year,'dataevents.csv',sep='')
  ## save those ts to a csv for importing into a database
  write.csv(events,file=filename,row.names = FALSE)
}

#' load a broken events file
#'
#' pass in a file name (fully qualified) and it will be read as csv and convereted to a proper events dataframe
#'
#' for use fixing old broken files
#'
#' @param f filename
#' @return dataframe with events, timeseries, detector id
load.broken.events.file <- function (f) {
  events <- read.csv(file=f,stringsAsFactors=FALSE)
  ts <- as.POSIXct(strptime(events$ts,"%Y-%m-%d %H:%M:%S",tz='GMT'))
  data.frame(ts=ts,detector_id=events$detector_id, event=events$event,stringsAsFactors=FALSE)
}

#' Fix lame events in a dataframe
#'
#' This function will correct for the fact that sql can't do self
#' joins to compute start and end times.  I recently learned a way
#' around this, but anyway...  This code just makes sure that I have
#' thestart and the end time for each event.  It just shifts index by
#' one, and computes right now here.
#'
#' Splitting this out like this let me fix old files way back when
#'
#' @param events the dataframe holding the event rows
#' @param year the year of the data
#' @return a new dataframe with the event lists corected.
#'
fix.lame.events.df <- function(events,year){
  ## I made a mistake.  SQL **HATES** doing self joins to compute start and end times
  ## fix that mistake here
  events$endts <- events$ts
  if(length(events$ts)>1){
    events$endts[1:(length(events$ts)-1)] <- events$ts[-1]
  }
  events[length(events$ts),'endts'] <- as.POSIXct(ISOdatetime(year+1,1,1,7,0,0,tz='UTC'))
  events
}

#' Summarize  events
#'
#' block out the good periods and bad periods in time
#'
#' Will create the events by calling \code{\link{summarize.events}}
#' first, and then save those events by calling
#' \code{\link{save.events.file}}
#'
#' @param df.agg the aggregated dataframe
#' @param year the year of data
#' @param good.periods an index identifying which rows of df.agg
#' represent "good" periods of data
#' @param detector.id the detector's id
#' @param ts the time stamp vector
#' @param detector.type the type of the detector, defaults to vdsid,
#' other likely possibility is 'wim'
#' @param con a database connection to use for valid dbnames
#' @return a dataframe contining rows indicating whether a section of
#' time was observed or imputed, suitable for stashing in a file or
#' database
summarize.events <- function(df.agg,year,good.periods,detector.id,ts,detector.type='vdsid',con){
  ## fiddle about with events and save those too.  easier in R than sql
  ## create factors for each of the good periods
  events <- data.frame()
  if(length(good.periods[good.periods])==0){
    ## save something and be done
      events <- data.frame(ts=ts[1],endts=ts[length(ts)],
                           detector_id=paste(detector.type,detector.id,sep='_'),
                           event='imputed');
  }
  else   if(length(good.periods[!good.periods])==0){
    ## all good
    events <- data.frame(ts=ts[1],endts=ts[length(ts)],
                         detector_id=paste(detector.type,detector.id,sep='_'),
                         event='good');
  }else{
    other <- data.frame(ts=df.agg$ts)
    other[good.periods,'goodbad'] <- 1
    other[!good.periods,'goodbad'] <- -1

    ## make indices, shift, and subtract to id the shift points (events)
    slough <- other$goodbad[-1]
    snow <- other$goodbad[1:(length(other$goodbad)-1)]
    zeros <- (slough +  snow) == 0
    event.index <- c(1,(2:length(other$ts))[zeros])
    events <- data.frame(ts = other$ts[event.index],
                         detector_id=paste(detector.type,detector.id,sep='_'))
    events$event[other$goodbad[event.index]==-1] <- 'imputed'
    events$event[other$goodbad[event.index]==1] <- 'observed'
  }
  db.legal.names  <- RPostgreSQL::make.db.names(con,
                                   names(events),
                                   unique=TRUE,
                                   allow.keywords=FALSE)
  names(events) <- db.legal.names
  events <- fix.lame.events.df(events,year)
  ## correct that last event end time.
  events$endts[length(events$endts)] <- df.agg$ts[length(df.agg$ts)]
  events
}
