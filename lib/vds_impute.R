junk.shot <- function(vds.id,path,fname,seconds,year,df){
  target.file <-  make.amelia.output.file(path,fname,seconds,year)
  reject <- 'reject'
  if( dim(df)[2] > 0 ){ ## don't get summary if there is nothing to summarize!
    reject <- data.frame(summary=summary(df))
  }
  save(reject,file=target.file)
  target.file <- make.db.dump.output.file(path,vds.id,year)
  dump <- data.frame(vds_id=vds.id)
  write.csv(dump,file=target.file,row.names = FALSE,col.names=TRUE,append=FALSE)
}

self.agg.impute.VDS.site.no.plots <- function(fname,f,path,year,seconds,goodfactor=2){
  ## aggregate, then impute

  ## return 1 if properly aggregated, return 0 if not
  returnval <- 0
  ## fname is the filename for the vds data.
  ## f is the full path to the file I need to grab

  ## is there a df available?
  ts <- data.frame()
  df <- data.frame()
  vds.id <-  get.vdsid.from.filename(fname)
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
    df$ts <- ts
    save(df,file=paste(path,'/',fname,'.df.',year,'RData',sep=''),compress='xz')
    df$ts <- NULL
  }

  if(sanity.check(df,ts)){
    gc()
    lanes <- longway.guess.lanes(df)
    print(paste('agg.impute.vds.site,',fname,', lanes',lanes))

    df.vds.agg <- vds.aggregate(df,ts,lanes=lanes,seconds=seconds)
    good.periods <- df.vds.agg$obs_count==seconds/30   & ! is.na(df.vds.agg$obs_count)

    ## this is tricky.  So because I am summing above, I only keep
    ## those time periods that have a full seconds/30 (= 3600/30 = 120
    ## for an hour aggregate) observations because periods==120.
    ## Otherwise, if periods is less than 120, then I am not going to
    ## keep that hour of data.  A little bit wasteful of information,
    ## but the flip side is imputing every 30 seconds and that is not
    ## possible.

    print(paste(length(df.vds.agg[good.periods,'obs_count' ]),' good periods versus total periods of ', (length(df.vds.agg$obs_count)) ))
    if(length(df.vds.agg[good.periods,'obs_count' ]) > (length(df.vds.agg$obs_count)/goodfactor)){
      print ('imputing')
      n.idx <- vds.lane.numbers(lanes,c("n"))
      o.idx <- vds.lane.numbers(lanes,c("o"))
      o.cols <- (1:length(names(df.vds.agg)))[is.element(names(df.vds.agg), o.idx)]
      o.bds.len <- length(o.cols)
      o.bds <- matrix(c(o.cols,sort( rep(c(0, 1),o.bds.len))), nrow = o.bds.len, byrow=FALSE)

      df.vds.agg.imputed <- list()

      r <- try(
               df.vds.agg.imputed <-
               amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
                      lags =c(n.idx),leads=c(n.idx),cs="day",intercs=TRUE,
                      sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,300))
               )

####### these are various different things I tried with imputation  ###
      ## df.vds.1800.imputed.2 <-
      ##   amelia(df.vds.1800,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
      ##          lags =c(n.idx),leads=c(n.idx),cs="day",intercs=TRUE,
      ##          sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

      ## df.vds.agg.imputed5 <-
      ##   amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
      ##   cs="day",intercs=TRUE,
      ##   sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))


      ## amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
      ## lags=c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
      ## sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

      ## amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=3,autopri=0.001,
      ##        lags =c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
      ##        sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

      ## amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=5,autopri=0.001,
      ##        lags =c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
      ##        sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

      if(class(r) == "try-error") {
        returnval <- paste(r,'')
        ## junk shot !
        junk.shot(vds.id,path,fname,seconds,year,df)
        
        return(returnval)
      }

      if(df.vds.agg.imputed$code==1){
        ## that means good imputation
        target.file <-  make.amelia.output.file(path,fname,seconds,year)
        save(df.vds.agg.imputed,file=target.file,compress='xz')
        verify.db.dump(fname,path,year,seconds,df.vds.agg.imputed)
        store.amelia.chains(df.vds.agg.imputed,year,vds.id,'vdsraw')
        returnval <- 1
      }else{
        returnval <- paste(df.vds.agg.imputed$code,'message',df.vds.agg.imputed$message)
        print(paste("amelia not happy:",returnval))
      }
    }else{
      print ('not imputing, not enough good periods versus bad periods or something')
      returnval <- paste(length(df.vds.agg[good.periods,'obs_count' ]),'good periods vs', length(df.vds.agg$obs_count),'total periods')
    }
    ## df.vds.agg
  }else{
    print ('not okay to process this file' )
    returnval <- "raw data failed basic sanity checks"
    ## junk shot !
    junk.shot(vds.id,path,fname,seconds,year,df)
  }
  returnval
}
