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

#' self aggregate and impute VDS data with no plotting
#'
#' This function loads raw data, aggregates it to some number of
#' seconds (a parameter passed in) then uses Amelia to impute missing
#' values.  No diagnostic plots are run.  Do those after.
#'
#' @param fname filename
#' @param f the root of the file name
#' @param path the path
#' @param year the year
#' @param seconds probably 120 these days
#' @param goodfactor defaults to 2
#' @param maxiter defaults to 100, currently using 20 as experiece
#' shows that if it goes past 20, it isn't going to converge at 100 or
#' 200 either, usually.
#' @param con a database connection to postgresql...no longer used
#' @param trackingdb the couchdb tracking db
#' @return a string indicating what failed or success
#' @export
self.agg.impute.VDS.site.no.plots <- function(fname,f,path,year,seconds,goodfactor=2,maxiter=100,con,trackingdb){
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

        df.30 <- vds.aggregate(df,ts,lanes=lanes,seconds=30)

        ## detect and remove any "no data" periods
        df.30 <- good.high.clustering.vds(df.30)

        ## properly drop missing times
        n.idx <- vds.lane.numbers(lanes,c("n"))
        o.idx <- vds.lane.numbers(lanes,c("o"))
        keep_time <- rep(TRUE,times=length(df.30$ts))
        for(laneidx in 1:lanes){
            keep_time <- keep_time &
                ## drop those time periods where volume is NA (missing)
                ! is.na(df.30[,n.idx[laneidx]] ) &
                ## also drop all times when volume is zero and occupancy is 1
                ! ( df.30[,n.idx[laneidx]] == 0 & df.30[,o.idx[laneidx]] == 1 )
        }

        ## aggregate up to requested seconds
        df.vds.agg <- vds.aggregate(df.30[keep_time,]
                                   ,df.30$ts[keep_time]
                                   ,lanes=lanes
                                   ,seconds=seconds)


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

            ## junk shot !
            junk.shot(vds.id,f,fname,seconds,year,df)

            print ('not imputing, not enough good periods versus bad periods or something')
            returnval <- paste(length(df.vds.agg[good.periods,'obs_count' ]),'good periods vs', length(df.vds.agg$obs_count),'total periods at agglevel of',seconds,'seconds')

        }else{

            print ('imputing')
            n.idx <- vds.lane.numbers(lanes,c("n"))
            o.idx <- vds.lane.numbers(lanes,c("o"))


            ## create o/n variable
            names_o_n <- paste(o.idx,'times',n.idx,sep='_')
            df.vds.agg[,names_o_n] <- df.vds.agg[,o.idx]*(df.vds.agg[,n.idx])

            o.cols <- (1:length(names(df.vds.agg)))[is.element(names(df.vds.agg), o.idx)]
            o.bds.len <- length(o.cols)
            o.bds <- matrix(c(o.cols,sort( rep(c(0, 1),o.bds.len))), nrow = o.bds.len, byrow=FALSE)

            df.vds.agg.imputed <- list()

            r <- try(
                df.vds.agg.imputed <-
                    Amelia::amelia(df.vds.agg,
                                   idvars=c('ts','obs_count'),
                                   ts="tod",
                                   splinetime=6,
                                   autopri=0.001,
                                   lags =c(n.idx,o.idx),
                                   leads=c(n.idx,o.idx),
                                   cs="day",
                                   intercs=TRUE,
                                   sqrts=c(n.idx,o.idx,names_o_n),
                                   bounds=o.bds,
                                   max.resample=10,
                                   emburn=c(2,maxiter))
                )



####### these are various different things I tried with imputation  ###
            ## df.vds.1800.imputed.2 <-
            ##   Amelia::amelia(df.vds.1800,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
            ##          lags =c(n.idx),leads=c(n.idx),cs="day",intercs=TRUE,
            ##          sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

            ## df.vds.agg.imputed5 <-
            ##   Amelia::amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
            ##   cs="day",intercs=TRUE,
            ##   sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))


            ## Amelia::amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=6,autopri=0.001,
            ## lags=c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
            ## sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

            ## Amelia::amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=3,autopri=0.001,
            ##        lags =c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
            ##        sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

            ## Amelia::amelia(df.vds.agg,idvars=c('ts','obs_count'),ts="tod",splinetime=5,autopri=0.001,
            ##        lags =c(n.idx,o.idx),leads=c(n.idx,o.idx),cs="day",intercs=TRUE,
            ##        sqrts=n.idx, bounds=o.bds,max.resample=10,emburn=c(2,2000))

            if(class(r) == "try-error") {
                returnval <- paste(r,'')
                ## junk shot !
                junk.shot(vds.id,f,fname,seconds,year,df)

                return(returnval)
            }

            ## this will mess up amelia-native plots and diagnositcs
            ## ## remove the v*n variables from each imputation
            ## for(i in 1:length(df.vds.agg.imputed$imputations)){
            ##     for(name_o_n in names_o_n){
            ##         df.vds.agg.imputed$imputations[[i]][,name_o_n] <- NULL
            ##     }
            ## }

            ## save no matter whether okay or bad
            savepath <- get.save.path(f)
            target.file <-  make.amelia.output.file(savepath,fname,seconds,year)
            save(df.vds.agg.imputed,file=target.file,compress='xz')
            if(df.vds.agg.imputed$code==1){
                ## that means good imputation
                ##
                ## verify.db.dump writes out the imputation, sort of.
                ## It ignores lanes and instead sums up all volume and
                ## occupancy for the hour at the site.  Not sure what
                ## it's purpose is these days, as I NEVER ingest the
                ## data into postgresql any more, so I am commenting
                ## it out and generally moving towards striking it
                ## from this repository.
                ##
                ## verify.db.dump(fname,path,year,seconds,df.vds.agg.imputed,con)
                ##
                store.amelia.chains(
                    df.vds.agg.imputed,year,vds.id,
                    'vdsraw',maxiter=maxiter,db=trackingdb
                )

                returnval <- 1
            }else{
                returnval <- paste(df.vds.agg.imputed$code,'message',df.vds.agg.imputed$message)
                rcouchutils::couch.set.state(
                    year=year,id=vds.id,db=trackingdb,
                    doc=list(
                        'raw_imputation_code'=df.vds.agg.imputed$code,
                        'raw_imputation_message'=df.vds.agg.imputed$message
                        )
                    )
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
