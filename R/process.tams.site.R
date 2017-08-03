
## day.of.week <-    c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
## lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')



#' Process a TAMS site's data, including pre-plots, imputation of
#' missing values, and post-plots.
#'
#' This is the main routine.
#'
#' @param tams.site the tams site
#' @param year the year
#' @param seconds the number of seconds to aggregate.  Almost always
#'     will be 3600 (which is one hour)
#' @param preplot TRUE or FALSE, defaults to TRUE
#' @param postplot TRUE or FALSE, defaults to TRUE
#' @param impute TRUE or FALSE, defaults to TRUE
#' @param force.plot TRUE or FALSE.  whether to redo the plots even if
#'     they are already in couchdb
#' @param tams.path where the TAMS data can be found on the local file
#' @param trackingdb the usual "vdsdata\%2ftracking" system.  Default
#'     is '/data/backup/tams' because that is the directory on the
#'     machine I developed this function on
#' @param use_csv TRUE to load from CSV files, ignore RData; FALSE to try
#'     first to use RData files
#' @return either list(), or a list with directions, holding for each
#'     direction the results of running the Amelia job (either good
#'     Amelia result, or bad Also check the trackingdb for any mention
#'     of issues.
#' @export
process.tams.site <- function(tams.site,
                             year,
                             seconds=3600,
                             preplot=TRUE,
                             postplot=TRUE,
                             impute=TRUE,
                             force.plot=FALSE,
                             tams.path='/data/backup/tams/',
                             trackingdb='vdsdata%2ftracking',
                             use_csv=FALSE){

    print(paste('tams path is ',tams.path))
    hour <- 3600
    returnval <- list()
    if(!preplot & !postplot & !impute){
        print('nothing to do here, preplot, postplot, and impute all false')
        return(returnval)
    }

    print(paste('starting to process  tams site ',tams.site,
                'preplot',preplot,
                'postplot',postplot,
                'impute',impute))

    ## two cases.  One, I'm redoing work and can just skip to the
    ## impute step.  Two, I need to run impute

    ## stupid globals
    directions <- NULL
    tams.data <- NULL
    site.lanes <- NULL

    if(!use_csv){
        list.results <- load.tams.from.fs(tams.site,year,tams.path,trackingdb)
        if(length(list.results) == 2){
            tams.data <- list.results[[1]]
            site.lanes <- list.results[[2]]
        }
    }

    if(length(tams.data) == 0){
        print('loading from CSV files')
        tams.data <- load.tams.from.csv(tams.site=tams.site,year=year,tams.path=tams.path)
        if(length(tams.data) == 0 || dim(tams.data)[1] == 0){
            print(paste("no data found for",tams.site,year," from path ",tams.path))
            return(returnval)
        }
        gc()

        ## passing data to functions is a memory hog in R, so trying to be
        ## careful here
        tams.data <- clean.tams.csv.lanes(tams.data)


        site.lanes <- max(tams.data$lane)

        tams.data <- trim.to.year(tams.data,year)

        tams.data <- tams.recode.lane_dir(tams.data)

        tams.data <- tams.recode.lanes(tams.data)
        tams.data$keep <- FALSE
        ## need to do this here to save RAM (??)
        bad_hrs_set <- c()
        directions <-  levels(as.factor(tams.data$lane_dir))
        for(direction in directions){
            dir_idx <- tams.data$lane_dir == direction
            lanes <- levels(as.factor(tams.data[dir_idx,]$lane))
            for (l in lanes){
                dir_idx <- tams.data$lane_dir == direction
                lane_dir_idx <- tams.data$lane == l & dir_idx
                tams.data[lane_dir_idx,'timestamp_full']  <- date_rollover_bug(tams.data[lane_dir_idx,]$timestamp_full)
                gc()
                tams.data[lane_dir_idx,'hrly'] <-
                    as.numeric(
                        trunc(
                            as.POSIXct(
                                tams.data[lane_dir_idx,]$timestamp_full
                               ,tz='UTC')
                           ,"hours")
                    )

                bad_hrs <- timestamp_insanity_fix(tams.data[lane_dir_idx,])
                bad_hrs_set <- union(bad_hrs_set,bad_hrs)
                keepers <- ! is.element(tams.data[lane_dir_idx,]$hrly,bad_hrs)
                drops_num <- length(keepers[!keepers])
                prior_num <- length(keepers)
                print(paste('dropping',drops_num,'out of',prior_num,'total records (or',round(100*(drops_num/prior_num),2),'percent) due to duplicated hours'))

                tams.data[lane_dir_idx,][keepers,]$keep <- TRUE

            }
        }
        tams.data <- tams.data[tams.data$keep,]
        gc()

        tams.data <- tams.extra.vars(tams.data)
        tams.by.dir <- list()
        for(direction in directions){
            tams.data.hr.lane <- list() ## list for output results
            dir_idx <- tams.data$lane_dir == direction
            lanes <- levels(as.factor(tams.data[dir_idx,]$lane))
            for (l in lanes){
                dir_idx <- tams.data$lane_dir == direction
                lane_dir_idx <- tams.data$lane == l & dir_idx
                df_hourly <- reshape.tams.from.csv.by.dir.by.lane(
                    tams.data[lane_dir_idx,],l
                )
                tams.data <- tams.data[!lane_dir_idx,]
                gc()
                df_hourly$ts <- as.POSIXct(df_hourly$ts,origin = "1970-01-01", tz = "UTC")
                df_hourly$ts <- trunc(df_hourly$ts,units='hours')

                tams.data.hr.lane[[l]] <- df_hourly
            }
            tams.by.dir[[direction]] <- tams.data.hr.lane
        }
        ## get rid of all CSV leftovers (should be none)
        tams.data <- list()

        ## now that no more CSV weighing down the RAM, clean up the hourlies
        for (direction in directions){
            ## combine lane by lane aggregates by same hour, by making a
            ## df with all of the hours (min to max)
            tams.data.hr.lane <- tams.by.dir[[direction]]
            lanes <-  names(tams.data.hr.lane)

            mints <- min(tams.data.hr.lane[[1]]$ts)
            maxts <- max(tams.data.hr.lane[[1]]$ts)
            for (l in lanes[-1]){
                mints <- min(mints,min(tams.data.hr.lane[[l]]$ts))
                maxts <- max(maxts,max(tams.data.hr.lane[[l]]$ts))
            }
            all.ts <- seq(mints,maxts,by=hour)
            df.return <- tibble::tibble(ts=all.ts,marker=FALSE)

            ## make ts posixlt to enable merge below
            df.return$ts <- as.POSIXlt(df.return$ts)

            if(length(keepers[!keepers]) > 0){
                print(paste('second pass, dropping',length(keepers[!keepers]),'records due to duplicated hours'))
                tams.data[[direction]] <- df.return[keepers,]
            }else{
                tams.data[[direction]] <- df.return
            }

            for(l in lanes){
                hrly <- as.numeric(
                    trunc(
                        tams.data.hr.lane[[l]]$ts
                       ,"hours"))
                keepers <- ! is.element(hrly,bad_hrs_set)

                tams.data.hr.lane[[l]] <- tams.data.hr.lane[[l]][keepers,]
                tams.data.hr.lane[[l]]$marker <- TRUE
                df.return <- merge(df.return,tams.data.hr.lane[[l]],by=c('ts'),all=TRUE)
                df.return$marker <- df.return$marker.x | df.return$marker.y
                df.return$marker.x <- NULL
                df.return$marker.y <- NULL
            }
            print(dim(df.return))
            ## here, if any data collected at all in an hour for any lane,
            ## then any lanes with NA values should be zero.  The theory
            ## is that if any data comes in for any lane, then all of the
            ## detectors were on and working, just nothing was recorded,
            ## so the NA should be zero, not NA.  leaving it NA will cause
            ## Amelia to try to fill that missing value with something.
            ## the "marker" bit identifies hours with any data vs hours
            ## with no data
            ##

            for(col in names(df.return)){
                navalues <- is.na(df.return[,col]) & !is.na(df.return$marker)
                if(any(navalues)){
                    df.return[navalues,col] <- 0
                }
            }
            df.return$marker <- NULL
            df.return$ts <- as.POSIXct(df.return$ts)
            df.return <- add.time.of.day(df.return)
            tams.data[[direction]] <- df.return


            save.tams.rdata(tams.data[[direction]]
                           ,tams.site
                           ,direction
                           ,year
                           ,tams.path)

        }

    }
    directions <- names(tams.data)

    gc()

    for(direction in directions){
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        is.stored <- rcouchutils::couch.check.state(year=year,
                                                    id=cdb.tamsid,
                                                    state='lanes',
                                                    db=trackingdb)
        if(is.stored == 'todo'){
            print('saving lanes to couchdb for next time')
            rcouchutils::couch.set.state(year=year,
                                         id=cdb.tamsid,
                                         doc=list('lanes'=site.lanes),
                                         db=trackingdb)
        }
    }

    for(direction in directions){
        print(paste('processing direction',direction))
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        df.tams <- NULL
        df.tams <- tams.data[[direction]]

        if(length(dim(df.tams)) !=  2){
            ## probably have an issue here
            print(paste("no data loaded for",tams.site,direction,year," ---  skipping"))
            next
        }



        if(preplot){
            print('plotting raw data, pre impute')
            print(dim(df.tams))
            attach.files <- plot_tams.data(df.tams
                                          ,tams.site
                                          ,direction
                                          ,year
                                          ,lanes.count=site.lanes
                                          ,fileprefix='raw'
                                          ,subhead='\npre imputation'
                                          ,force.plot=force.plot
                                          ,trackingdb=trackingdb
                                          ,tams.path=tams.path)
            if(length(attach.files) > 1 || attach.files != 1){
                for(f2a in c(attach.files)){
                    rcouchutils::couch.attach(trackingdb,cdb.tamsid,f2a)
                }
            }
        }

        ## right here add sanity checks for data for example, if
        ## the hourly values are super high compared to the
        ## "usual" max value

        ## I needed to trim data in psql for WIM.  leave this here for
        ## future reference just in case
        ## and a month later, indeed I am using this!
        ## config <- rcouchutils::get.config()
        ## sqldf_postgresql(config)
        ## df.trimmed <- good.high.clustering(df.tams)
        ## and also do the plot -> couchdb thing again if needed

        df.tams.amelia <- NULL

        if(impute){

            print(paste('imputing',year,tams.site,direction))
            plotspath <- calvadrscripts::plot_path(tams.path = tams.path
                                                  ,year = year
                                                  ,site_no = tams.site
                                                  ,direction = direction
                                                  ,makedir = FALSE)
            plotsname <- paste(plotspath,paste('ameliaplots_',year,'.png',sep=''),sep='/')
            r <- try(
                df.tams.amelia <- fill.tams.gaps(df.tams=df.tams
                                                ,plotfile=plotsname)
            )
            if(class(r) == "try-error") {
                returnval[[direction]] <- paste(r,'')
                print(paste('try error:',r))
                rcouchutils::couch.set.state(year=year,
                                             id=cdb.tamsid,
                                             doc=list('imputed'=paste('try error',r)),
                                             db=trackingdb)
            }
            if(df.tams.amelia$code==1){
                ## that means good imputation
                ## have a TAMS site data with no gaps.  save it

                ## note that maxiter is currently hardcoded at 100 in
                ## tams.impute.functions, so that's why it says plain
                ## '100' below
                store.amelia.chains(
                    df.amelia=df.tams.amelia,
                    year=year,
                    detector.id=cdb.tamsid,
                    imputation.name='tamsraw',
                    maxiter=100,
                    db=trackingdb
                )

                ## save generated plots too
                amelia.plots <- dir(plotspath,pattern='ameliaplots'
                                   ,full.names=TRUE,all.files=TRUE)
                for(f2a in c(amelia.plots)){
                    result <- rcouchutils::couch.attach(trackingdb,cdb.tamsid,f2a)
                }

                savepath <- paste(tams.path,year,sep='/')
                if(!file.exists(savepath)){dir.create(savepath)}
                savepath <- paste(savepath,tams.site,sep='/')
                if(!file.exists(savepath)){dir.create(savepath)}
                savepath <- paste(savepath,direction,sep='/')
                if(!file.exists(savepath)){dir.create(savepath)}

                target.file <- make.amelia.output.file(savepath,paste('tams',tams.site,direction,sep=''),seconds,year)
                print(paste('name is',target.file))
                ## fs write
                save(df.tams.amelia,file=target.file,compress="xz")
                rcouchutils::couch.set.state(year=year,
                                             id=cdb.tamsid,
                                             doc=list('imputed'='finished'),
                                             db=trackingdb)

            }else{
                errdoc <- paste(df.tams.amelia$code,
                                'message',df.tams.amelia$message)
                print(paste("amelia not happy:",errdoc))
                rcouchutils::couch.set.state(year=year,
                                             id=cdb.tamsid,
                                             doc=list('imputed'=
                                                          paste('error:',
                                                                errdoc)),
                                             db=trackingdb)
            }

        }else{
            ## not imputing this run, but maybe post plotting
            if(postplot){
                print('load imputed data from filesystem')
                df.tams.amelia <- get.amelia.tams.file.local(tams.site=tams.site
                                                            ,year=year
                                                            ,direction=direction
                                                            ,tams.path=tams.path)

            }
        }

        returnval[[direction]] <- df.tams.amelia

        if(postplot){
            print('plotting post-imputation')
            df.tams.agg.amelia <- tams.medianed.aggregate.df(df.tams.amelia)
            attach.files <- plot_tams.data(df.tams.agg.amelia
                                          ,tams.site
                                          ,direction
                                          ,year
                                          ,fileprefix='imputed'
                                          ,lanes.count = site.lanes
                                          ,subhead='\npost imputation'
                                          ,force.plot=TRUE
                                          ,trackingdb=trackingdb
                                          ,tams.path=tams.path)
            if(length(attach.files) > 1 || attach.files != 1){
                for(f2a in c(attach.files)){
                    rcouchutils::couch.attach(trackingdb,cdb.tamsid,f2a)
                }
            }
        }
    }

    returnval
}
