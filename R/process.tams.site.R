
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
        tams.data <- load.tams.from.fs(tams.site,year,tams.path,trackingdb)
    }
    if(length(tams.data) != 2){
        print('loading from CSV files')
        tams.data <- load.tams.from.csv(tams.site=tams.site,year=year,tams.path=tams.path)
        if(length(tams.data) == 0 || dim(tams.data)[1] == 0){
            print(paste("no data found for",tams.site,year," from path ",tams.path))
            return(returnval)
        }

        tams.data <- reshape.tams.from.csv(tams_csv=tams.data,year=year,tams.path = tams.path)
        site.lanes <- tams.data[[2]]
        tams.data <- tams.data[[1]]
    }else{
        site.lanes <- tams.data[[2]]
        tams.data <- tams.data[[1]]
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
