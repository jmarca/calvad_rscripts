source("load.pems.raw.file.R")
source("vds.processing.functions.R")
source("vds.aggregate.R")
##source('components/jmarca/rstats_couch_utils/master/couchUtils.R')


store.amelia.chains <- function(df.amelia,year,detector.id,imputation.name='',maxiter=100){
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
  couch.set.state(year=year,detector.id=detector.id,doc=trackerdoc)
  return (itercount)
}


junk.shot <- function(vds.id,f,fname,seconds,year,df){
    savepath <- get.save.path(f)

    target.file <-  make.amelia.output.file(savepath,fname,seconds,year)
    reject <- 'reject'
    if( dim(df)[2] > 0 ){ ## don't get summary if there is nothing to summarize!
        reject <- data.frame(summary=summary(df))
    }
    save(reject,file=target.file)
    ## target.file <- make.db.dump.output.file(savepath,vds.id,year)
    ## dump <- data.frame(vds_id=vds.id)
    ## write.csv(dump,file=target.file,row.names = FALSE,col.names=TRUE,append=FALSE)
}

self.agg.impute.VDS.site.no.plots <- function(fname,f,path,year,seconds,goodfactor=2,maxiter=100){
    ## aggregate, then impute
    vds.id <-  get.vdsid.from.filename(fname)

    ## return 1 if properly aggregated, return 0 if not
    returnval <- 0
    ## fname is the filename for the vds data.
    ## f is the full path to the file I need to grab

    df <- load.file(f,fname,year,path)
    ## break out times
    ts <- df$ts
    df$ts <- NULL

    if(sanity.check(df,ts,year=year,vdsid=vds.id)){
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

        ## decide whether or not to impute

        if(length(df.vds.agg[good.periods,'obs_count' ]) < (length(df.vds.agg$obs_count)/goodfactor)){

            print ('not imputing, not enough good periods versus bad periods or something')
            returnval <- paste(length(df.vds.agg[good.periods,'obs_count' ]),'good periods vs', length(df.vds.agg$obs_count),'total periods at agglevel of',seconds,'seconds')

        }else{

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
                       sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,maxiter))
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
                junk.shot(vds.id,f,fname,seconds,year,df)

                return(returnval)
            }

            if(df.vds.agg.imputed$code==1){
                ## that means good imputation
                savepath <- get.save.path(f)
                target.file <-  make.amelia.output.file(savepath,fname,seconds,year)
                save(df.vds.agg.imputed,file=target.file,compress='xz')
                verify.db.dump(fname,path,year,seconds,df.vds.agg.imputed)
                store.amelia.chains(df.vds.agg.imputed,year,vds.id,'vdsraw',maxiter=maxiter)
                returnval <- 1
            }else{
                returnval <- paste(df.vds.agg.imputed$code,'message',df.vds.agg.imputed$message)
                print(paste("amelia not happy:",returnval))
            }
            ## df.vds.agg
        }
    }else{

        print ('not okay to process this file' )
        returnval <- "raw data failed basic sanity checks"
        ## junk shot !
        junk.shot(vds.id,f,fname,seconds,year,df)

    }
    returnval
}
