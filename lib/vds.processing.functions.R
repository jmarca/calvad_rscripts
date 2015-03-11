source('./utils.R')

sanity.check <- function(data,ts,year=0,vdsid='missing'){
    problem <- list()

  return.val <- dim(data)[2] > 0  ## catch empty data right away

    if(!return.val){
        problem['rawdata'] <- 'no rows of data in raw vds file'
    }
    if(return.val){
        return.val <- is.element("nr1",names(data))    ## sometimes get random interior lanes
        if(!return.val){
            problem['rawdata'] <- 'have data, but not right hand lane? in raw vds file'
        }
    }

    if(return.val){
        names.vds <- names(data)
        max.lanes <- 8
        lane <- 0
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
                return.val <- FALSE
            }
            if( is.element(paste("sl",lane), names.vds ) & (! is.element(paste("nl",lane), names.vds ) & ! is.element(paste("ol",lane), names.vds ) ) ){
                problem['rawdata'] <- paste('have a speed value, but not both occupancy and counts for lane',paste("nr",lane),'in raw vds file')
                return.val <- FALSE
            }
        }
    }
    if(return.val){
        ## finally, can't do any imputation unless you have at least a month of data
        difference <- difftime(ts[length(ts)],ts[1],units='weeks')
        if(difference < 4){
            problem['rawdata'] <- paste('need more than 4 weeks of data, have only',difference,'weeks','in raw vds file')
            return.val <- FALSE
        }
    }
    if(return.val){ ## still going good, do some more checks
        lanes <- longway.guess.lanes(data)
        n.idx <- vds.lane.numbers(lanes,c("n"))
        if(! length(data$nl1) > 0){
            problem['rawdata'] <- paste('do not have counts in left lane','in raw vds file')
            return.val <- FALSE
        } else {
            if(! length(data$ol1) > 0 ){
                problem['rawdata'] <- paste('do not have occupancies in left lane','in raw vds file')
                return.val <- FALSE
            } else {
                mean.ns <- sapply(data[n.idx],mean,na.rm=TRUE)
                bad.lanes <-  length(n.idx[( mean.ns <0.0001)])
                if(! bad.lanes == 0 ){
                    problem['rawdata'] <- paste('mean volumes too low in some lanes:',paste(n.idx[( mean.ns <0.0001)],mean.ns[( mean.ns <0.0001)],sep=':',collapse=', '),'in raw vds file',collapse=' ')
                    return.val <- FALSE
                }
            }
        }
    }
    if(return.val){ ## check that we're not stuck on zero
        return.val <- max(data$nl1,na.rm=TRUE)>0
        if(!return.val){
            problem['rawdata'] <- paste('left lane max count is zero','in raw vds file')
        }
    }
    if(return.val){ ## check for 4 weeks of raw data
        return.val <- length(data[!is.na(data[1]),1]) > 2*60*24*7*4 # 4 weeks * 2 obs/min * 60 min/hr * 24 hr/day * 7 days/week
        if(!return.val){
            weeks.data <- length(data[!is.na(data[1]),1]) / 2*60*24*7
            problem['rawdata'] <- paste('need at least 4 weeks of raw data total.  Have only',weeks.data,'in raw vds file')
        }
    }
    if(!return.val){
        ## save to couchdb
        if(year != 0 & vdsid != 'missing' ){
            couch.set.state(year,vdsid,doc=problem,local=TRUE)
        }

    }
    return.val
}


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

recode.lanes <- function(df){
                                        # run this only after you've
                                        # run trim empty lanes

  ##
  ## recode to be right lane (r1), right lane but one (r2), r3, ... and then
  ## left lane (l1)
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

db.ready.dump <- function(imputations.list,vds.id,path='.'){
  lanes <- longway.guess.lanes(imputations.list[[1]])
  target.file <- make.db.dump.output.file(path,vds.id,year)
  dump.file.size <- file.info(target.file)$size
  for(imps in 1:(length(imputations.list))){
    imp <- imputations.list[[imps]]

    names.vds <- names(imp)
    n.idx <- vds.lane.numbers(lanes,c("n"))
    o.idx <- vds.lane.numbers(lanes,c("o"))
    s.idx <- vds.lane.numbers(lanes,c("s"))
    s.idx <- names(imputations.list[[1]])[is.element( names(imputations.list[[1]]),s.idx)]
    ## if there are not speeds, then s.idx[1] is NA

    dump <- data.frame(vds_id=vds.id,ts=imp$ts)
    dump$obs_count <-  imp$obs_count
    dump$imputation <- imps
    if(lanes==1){
      dump$vol <-  imp[,n.idx]
      dump$occ <-  imp[,o.idx]
    }else{
      dump$vol <-  apply(imp[,n.idx], 1, sum)
      dump$occ <-   apply(imp[,o.idx], 1, mean)
    }

    if(is.na(s.idx[1])){
      dump$spd <- NA
    }else{
      ## I just relearned that * is element by element multiplication
      weighted <- as.matrix(imp[,n.idx]) * as.matrix(imp[,s.idx])
      if(lanes==1){
        dump$spd <- weighted / dump$vol
      }else{
        dump$spd <- apply(weighted, 1, sum) / dump$vol
      }
    }

    dump$sd_vol <- NA
    dump$sd_occ <- NA
    dump$sd_spd <- NA

    db.legal.names  <- make.db.names(con,names(dump),unique=TRUE,allow.keywords=FALSE)
    names(dump) <- db.legal.names
    ## fs write

    ## need to append, not overwrite the target file for each imputation

    if(is.na(dump.file.size)){
      write.csv(dump,file=target.file,row.names = FALSE,col.names=TRUE,append=FALSE)
      dump.file.size <- file.info(target.file)$size
    }else{
      write.csv(dump,file=target.file,row.names = FALSE,col.names=FALSE,append=TRUE)
    }
  }
}

verify.imputation.was.okay <- function(fname,path,year,seconds,df.vds.agg.imputed=NA){
  amelia.dump.file <- make.amelia.output.pattern(fname,year)
  done.file <- dir(path, pattern=amelia.dump.file,
                   full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
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

verify.db.dump <- function(fname,path,year,seconds,df.vds.agg.imputed=NA){
  vds.id <-  get.vdsid.from.filename(fname)
  target.file <- make.db.dump.output.file(path,vds.id,year)
  load.result='okay'
  if(is.na(file.info(target.file)$size)){
    ## no ticket, no pass
    ## load the fname, get the amelia output, dump it
    if(is.na(df.vds.agg.imputed)){
      df.vds.agg.imputed <- get.vds.file(vds.id,path,year)
    }

    aout.agg <- data.frame(vds_id=vds.id)
    ## only go if df.vds.agg.imputed is sane
    if(load.result!='reject' & ( length(df.vds.agg.imputed$imputations)>1 & df.vds.agg.imputed$code==1)){
      aout.agg <- impute.aggregate(df.vds.agg.imputed)
    }
    db.ready.dump(aout.agg,vds.id,path)
  }
}


get.vds.file <- function(vds.id,path,year){

  amelia.dump.file <- make.amelia.output.pattern(vds.id,year)
  files <- dir(path, pattern=amelia.dump.file,
                   full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
  df.vds.agg.imputed <- list();
  if(length(files)>0){
    print(paste('loading stored vds amelia object from file',files[1]))
    load.result <-  load(file=files[1])
  }
  df.vds.agg.imputed
}


make.db.dump.output.file <- function(path,vds.id,year){
  paste(path,paste('vds_hour_agg',vds.id,year,'dat',sep='.'),sep='/')
}

get.vdsid.from.filename <- function(filename){
  ## files format is [vdsid]_[vdstype]_[year]
  vds.id <-  strsplit(filename,"_")[[1]][1]
  vds.id
}


impute.aggregate <- function(aout,hour=3600){
  ## this function will take an amelia output object, and then return
  ## a data frame with the imputed data aggregated up to one hour
  ## (3600 seconds)
  lanes <- longway.guess.lanes(aout$imputations[[1]])
  print(paste('in impute.aggregate, with lanes=',lanes))
  n.idx <- c(vds.lane.numbers(lanes,c("n")),'obs_count')
  n.idx.only <- vds.lane.numbers(lanes,c("n"))
  o.idx <- vds.lane.numbers(lanes,c("o"))
  s.idx <- vds.lane.numbers(lanes,c("s"))
  s.idx <- names(aout$imputations[[1]])[is.element( names(aout$imputations[[1]]),s.idx)]
  ## if there are not speeds, then s.idx[1] is NA
  aggimp = list()
  for(imps in 1:(length(aout$imputations))){
    imp <- aout$imputations[[imps]]
    ## I don't really care about seconds in the original, as long as
    ## it is true that it is less than 3600, as the ts is already in
    ## the amelia output, and I just want to aggregate up.
    ##

    df.zoo.n <- zooreg(imp[,n.idx],order.by=imp$ts)
    df.zoo.n <-  aggregate(df.zoo.n,
                           as.numeric(time(df.zoo.n)) -
                           as.numeric(time(df.zoo.n)) %% hour,
                           sum, na.rm=TRUE)

    df.zoo.o <- zooreg(imp[,o.idx],order.by=imp$ts)
    df.zoo.o <-  aggregate(df.zoo.o,
                           as.numeric(time(df.zoo.o)) -
                           as.numeric(time(df.zoo.o)) %% hour,
                           sum, ## mean,
                           na.rm=TRUE)

    ## more cut and paste and tweak programming
    if(! is.na(s.idx[1]) ){
      ## speed is in the dataset.  Weight by vehicle count
      df.weighted.s = data.frame(imp[,n.idx.only])
      df.weighted.s[,s.idx] <- imp[,s.idx] * imp[,n.idx.only]
      names(df.weighted.s) = c(n.idx.only,s.idx)
      df.zoo.s <- zooreg(df.weighted.s,order.by=imp$ts)
      df.zoo.s <-  aggregate(df.zoo.s,
                             as.numeric(time(df.zoo.s)) -
                             as.numeric(time(df.zoo.s)) %% hour,
                             sum, na.rm=TRUE)
      ## for(i in 1:(length(s.idx))){
      ##   df.zoo.s[,s.idx[i]] <- df.zoo.s[,s.idx[i]]/df.zoo.s[,n.idx.only[i]]
      ## }
      df.zoo.o <- merge( df.zoo.o,df.zoo.s[,s.idx] )
      names(df.zoo.o) <- c(o.idx,s.idx)
    }
    ## merge sets n and o (plus s)
    aggregate.combined <- merge( df.zoo.n,df.zoo.o, suffixes = c("sum", "sumave"))

    if(! is.na(s.idx[1]) ){
      names(aggregate.combined) <- c(n.idx,o.idx,s.idx)
    }else{
      names(aggregate.combined) <- c(n.idx,o.idx)
    }

    df.agg <- data.frame(coredata(aggregate.combined[,names(aggregate.combined)]))
    df.agg$ts <- unclass(time(aggregate.combined))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
    ts.lt <- as.POSIXlt(df.agg$ts)
    df.agg$tod   <- ts.lt$hour + (ts.lt$min/60)
    df.agg$day   <- ts.lt$wday
    aggimp[[imps]] <- df.agg
  }
  aggimp
}


hourly.agg.VDS.site <- function(fname,f,path,year,vds.id){
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
  isa.df <- dir(path, pattern=target.file,full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
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
  db.legal.names  <- make.db.names(con,names(other),unique=TRUE,allow.keywords=FALSE)
  names(other) <- db.legal.names
  filename <- paste(path,'/',fname,'.',year,'rawaggregate.csv',sep='')
  write.csv(other,file=filename,row.names = FALSE)
  rm(other)

  events <- summarize.events(df.vds.agg,year,good.periods,vds.id,ts)
  save.events.file(path,fname,year,events)
}

save.events.file <- function(path,fname,year,events){
  filename <- paste(path,'/',fname,'.',year,'dataevents.csv',sep='')
  ## save those ts to a csv for importing into a database
  write.csv(events,file=filename,row.names = FALSE)
}

load.broken.events.file <- function (f) {
  events <- read.csv(file=f,stringsAsFactors=FALSE)
  ts <- as.POSIXct(strptime(events$ts,"%Y-%m-%d %H:%M:%S",tz='GMT'))
  data.frame(ts=ts,detector_id=events$detector_id, event=events$event,stringsAsFactors=FALSE)
}

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

summarize.events <- function(df.agg,year,good.periods,detector.id,ts,detector.type='vdsid'){
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
    events <- data.frame(ts = other$ts[event.index], detector_id=paste(detector.type,detector.id,sep='_'))
    events$event[other$goodbad[event.index]==-1] <- 'imputed'
    events$event[other$goodbad[event.index]==1] <- 'observed'
  }
  db.legal.names  <- make.db.names(con,names(events),unique=TRUE,allow.keywords=FALSE)
  names(events) <- db.legal.names
  events <- fix.lame.events.df(events,year)
  ## correct that last event end time.
  events$endts[length(events$endts)] <- df.agg$ts[length(df.agg$ts)]
  events
}
