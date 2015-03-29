source('./utils.R')
source('./wim.impute.functions.R')
source('./wim.aggregate.fixed.R')
source('./wim.loading.functions.R')

source('./amelia_plots_and_diagnostics.R')
source('./get.medianed.amelia.vds.R')
pf <- function(x,y){panel.smoothScatter(x,y,nbin=c(200,200))}

day.of.week <-    c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')
strip.function.a <- strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )

#' load the imputed WIM (Amelia) output object from the file system
#'
#' Will NOT aggregate or clean up the Amelia output.  Just gets it
#'
#' @param wim.site
#' @param year
#' @param direction
#' @param wim.path default '/data/backup/wim' because that is where it
#' goes on the machine I developed this on.
#' @param seconds the number of seconds that were aggregated up for
#' the Amelia run.  defaults to one hour, or 3600
#' @return the amelia output object
load_imputed_wim <- function(wim.site,year,direction,wim.path='/data/backup/wim',seconds=3600){
    ## reload the imputed wim data
    cdb.wimid <- paste('wim',wim.site,direction,sep='.')
    savepath <- paste(wim.path,year,wim.site,direction,sep='/')
    target.file <- make.amelia.output.file(savepath,
                                           paste('wim',wim.site,direction,sep=''),
                                           seconds,year)
    print(paste('loading',target.file))
    ## fs read
    df.wim.amelia <- NULL
    load.result <- load(file=target.file)
    df.wim.amelia
}

#' handle a wim site, direction.  This function loads the imputed wim
#' data, then makes a merged file, makes truck plots, and attaches
#' them to the couchdb
#'
#' @param wim.site the WIM site id
#' @param year
#' @param direction the direction for this data
#' @param wim.path where are the saved files to be found on the local file system
#' @param seconds how much the WIM data was aggregated up for the
#' Amelia runs...defaults to one hour, or 3600 seconds
#' @param trackingdb defaults to the usual 'vdsdata%2ftracking'
#' @return nothing or nothing.  run this for the side effect of
#' generating plots to couchdb
handle_wim_dir <- function(wim.site,
                           year,
                           direction,
                           wim.path='/data/backup/wim',
                           seconds=3600,
                           trackingdb='vdsdata%2ftracking'){
    df.wim.amelia <- load_imputed_wim(wim.site,year,direction,wim.path,seconds)
    if(length(df.wim.amelia) == 1){
        print(paste("amelia run for wim not good",df.wim.amelia))
        return(NULL)
    }else if(!length(df.wim.amelia)>0 ||
             !length(df.wim.amelia$imputations)>0 ||
             df.wim.amelia$code!=1 ){
        print("amelia run for vds not good")
        return(NULL)
    }
    df.wim.amelia.c <- NULL
    for(i in 1:length(df.wim.amelia$imputations)){
        df.wim.amelia.c <- rbind(df.wim.amelia.c,df.wim.amelia$imputations[[i]])
    }
    df.merged <- medianed.aggregate.df(df.wim.amelia.c)
    print('make plots')
    files.to.attach <- make.truck.plots(df.merged,year,wim.site,
                                        direction,cdb.wimid,imputed=TRUE)

    upload.plots.couchdb(trackingdb=trackingdb,
                         files.to.attach=files.to.attach)
    1
}

#' Generate the post imputation plots for a WIM site
#'
#' will run through all directions of flow defined for this WIM site
#' for the given year
#'
#' @param wim.site the WIM site id
#' @param year
#' @param wim.path where is the data stashed
#' @param seconds the number of seconds aggregated in the data,
#' defaults to one hour or 3600
#' @param trackingdb where to shove the plots as attachments, defaults
#' to the usual couchdb trackingdb of 'vdsdata%2ftracking'
#' @return 1 Run this for the side effect or generating plots for all
#' directions at this WIM site
#'
post.impute.plots <- function(wim.site,
                              year,
                              wim.path='/data/backup/wim',
                              seconds=3600,
                              trackingdb='vdsdata%2ftracking'){
    ## no need to load raw data
    df.directions <- get.wim.directions(wim.site)
    directions = df.directions$direction
    print(paste(directions,collapse=','))
    for(direction in directions){
        handle_wim_dir(wim.site,year,direction,wim_path,seconds)
    }
    1
}



## must modularize this more

process.wim.site <- function(wim.site,year,preplot=TRUE,postplot=TRUE,impute=TRUE,wim.path='/data/backup/wim/'){

    print(paste('wim path is ',wim.path))

    returnval <- 0
    if(!preplot & !postplot & !impute){
        print('nothing to do here, preplot, postplot, and impute all false')
        return(0)
    }

    print(paste('starting to process  wim site ',wim.site))

    ## two cases.  One, I'm redoing work and can just skip to the
    ## impute step.  Two, I need to hit the db directly.  Figure it
    ## out by checking if I can load the data from the file

    directions <- get.wim.directions(wim.site)

    df.wim <- load.wim.data.straight(wim.site,year)
    ## only continue if I have real data
    if(dim(df.wim)[1]==0){
        print(paste('problem, dim df.wim is',dim(df.wim)))
        couch.set.state(year=year,detector.id=paste('wim',wim.site,sep='.'),doc=list('imputed'='no wim data in database'))
        return(0)
    }

    df.wim.split <- split(df.wim, df.wim$direction)
    directions <- names(df.wim.split)
    df.wim.speed <- get.wim.speed.from.sql(wim.site,seconds,year)
    df.wim.speed.split <- split(df.wim.speed, df.wim.speed$direction)
    rm(df.wim)
    rm(df.wim.speed)

    df.wim.dir <- list()
    for(direction in directions){
        print(paste('processing direction',direction))
        ## direction <- names(df.wim.split)[1]
        cdb.wimid <- paste('wim',wim.site,direction,sep='.')
        if(length(df.wim.split[[direction]]$ts)<100){
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='less than 100 timestamps for raw data in db'))
            next
        }
        if(length(df.wim.speed.split[[direction]]$ts)<100){
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='less than 100 timestamps for speed data in db'))
            next
        }

        ## for output files
        if(!file.exists(paste("images",direction,sep='/'))){
            dir.create(paste("images",direction,sep='/'))
        }
        df.wim.d <- process.wim.2(df.wim.split[[direction]])
        df.wim.s <- df.wim.speed.split[[direction]]

        ## fix for site 16, counts of over 100,000 per hour (actually 30 million)
        too.many <- df.wim.s$veh_count > 10000 ## 10,000 veh in 5 minutes!
        df.wim.s <- df.wim.s[!too.many,]


        df.wim.split[[direction]] <- NULL
        df.wim.speed.split[[direction]] <- NULL
        ## gc()

        df.wim.d <- wim.additional.variables(df.wim.d)

        ## aggregate over time
        print(' aggregate ')
        df.wim.dagg <-wim.lane.and.time.aggregation(df.wim.d)
        ## hack around broken speed summaries but instead aborting above.
        ## The one such instance so far had junk measurements
        if(length(df.wim.s)==0){
            ## insert one dummy record per lane
            lastlane <- max(df.wim.d$lane)
            dummytime <- df.wim.d$ts[1]
            df.wim.s <- data.frame(cbind(lane=c('l1',paste('r',2:lastlane,sep=''))))
            df.wim.s$ts <- dummytime
            df.wim.s$veh_speed <- NA
            df.wim.s$veh_count <- NA

        }
        df.wim.sagg <- make.speed.aggregates(df.wim.s)

        ## merge, then explode time using zoo
        df.wim.d.joint <- merge(df.wim.dagg,df.wim.sagg)
        rm(df.wim.dagg, df.wim.sagg,df.wim.s,df.wim.d )
        gc()

        col.names <- names(df.wim.d.joint)
        local.df.wim.agg.ts <- as.ts(df.wim.d.joint)
        rm(df.wim.d.joint)
        ts.ts <- unclass(time(local.df.wim.agg.ts))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
        local.df.wim.agg <- data.frame(ts=ts.ts)
        local.df.wim.agg[col.names] <- local.df.wim.agg.ts ## [,col.names]
        rm(local.df.wim.agg.ts)
        local.df.wim.agg <- add.time.of.day(local.df.wim.agg)
        ##    df.wim.dir[[direction]] <- local.df.wim.agg

        ## gc()
        if(preplot){
            preplot(wim.site,direction,year,local.df.wim.agg)
        }
        if(impute){

            ## direction <- names(df.wim.split)[1]
            print(paste(year,wim.site,direction))
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='started'))

            ## save and move on to the next one

            ## but save where?
            ## to couchdb as hourly data??
            ## no, to fs on lysithia.  brief digression to write that into my node server.

            savepath <- paste(wim.path,year,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            savepath <- paste(savepath,wim.site,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            savepath <- paste(savepath,direction,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            filepath <- paste(savepath,'wim.agg.RData',sep='/')
            print(filepath)

            db.legal.names  <- gsub("\\.", "_", names(local.df.wim.agg))

            names(local.df.wim.agg) <- db.legal.names
            save(local.df.wim.agg,file=filepath,compress='xz')
            print(paste('saved to',filepath))

            print('amelia run')

            r <- try(
                df.wim.amelia <- fill.wim.gaps(local.df.wim.agg
                                               ,count.pattern='^(not_heavyheavy|heavyheavy|count_all_veh_speed)'
                                               )
                )
            if(class(r) == "try-error") {
                returnval <- paste(r,'')
                print(paste('try error:',r))
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'=paste('try error',r)))
            }

            if(df.wim.amelia$code==1){
                ## that means good imputation
                ## have a WIM site data with no gaps.  save it
                target.file <- make.amelia.output.file(savepath,paste('wim',wim.site,direction,sep=''),seconds,year)
                print(paste('name is',target.file,'savepath is',savepath))
                ## fs write
                save(df.wim.amelia,file=target.file,compress="xz")
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='finished'))
                returnval <- 1
            }else{
                returnval <- paste(df.vds.agg.imputed$code,'message',df.vds.agg.imputed$message)
                print(paste("amelia not happy:",returnval))
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'=paste('error:',returnval)))
            }
            rm(df.wim.amelia,local.df.wim.agg)
            gc()
        }

    }

    returnval
}

#' Generate the plots before imputation
#'
#' That's why it is called "pre"
#'
#' @param wim.site the WIM site id
#' @param direction the direction of flow for this data
#' @param year
#' @param df.wim the WIM data dataframe
#' @param imagepath where to stash the images generated, defaults to "./images/"
#' @param trackingdb defaults to the usual 'vdsdata%2ftracking'
#' @return falls of the end and dies.  Run for the side effect of
#' generating files in the local filesystem that then also get
#' uploaded to CouchDB tracking database document for this wim site as
#' attachments
preplot <- function(wim.site,direction,year,df.wim,imagepath="images/",
                    trackingdb='vdsdata%2ftracking'){

    file.pattern <- paste(wim.site,direction,year,'agg.redo',sep='_')

    file.path <- paste(paste(imagepath,direction,'/',sep=''),file.pattern,sep='')

    png(file = paste(file.path,'%03d.png',sep='_'), width=900, height=600, bg="transparent",pointsize=18)

    plotvars <- grep('not_heavyheavy_',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot non-heavy heavy duty truck counts",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Hour of the day"
               ,ylab="Non HHD truck counts per hour"
               ,panel=pf
               ,auto.key=TRUE)
    print(a)
    plotvars <- grep('^heavyheavy_',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot heavy heavy duty truck counts",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Hour of the day"
               ,ylab="HHD truck counts per hour"
               ,panel=pf
               ,auto.key=TRUE)
    print(a)

    plotvars <- grep('^count_all',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot counts from summary reports,",year,"  by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Hour of the day"
               ,ylab="Hourly vehicle counts, all classes"
               ,panel=pf
               ,auto.key=TRUE)
    print(a)

    splotvars <- grep('^wgt',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I( (', paste(splotvars,collapse='+' ),') / (', paste(plotvars,collapse='+' ),')) ~ tod | day'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot mean speeds from summary reports,",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Hour of the day"
               ,ylab="Hourly mean speeds"
               ,panel=pf
               ,auto.key=TRUE)
    print(a)

    ## add plots of data over time

    plotvars <- grep('not_heavyheavy_',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot non-heavy heavy duty truck counts",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Date"
               ,ylab="non HHD truck counts per hour"
               ,auto.key=TRUE)
    print(a)

    plotvars <- grep('^heavyheavy_',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot heavy heavy duty truck counts",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Date"
               ,ylab="HHD truck counts per hour"
               ,auto.key=TRUE)
    print(a)

    plotvars <- grep('^count_all',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot counts from summary reports,",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Date"
               ,ylab="Hourly vehicle counts, all classes"
               ,auto.key=TRUE)
    print(a)

    splotvars <- grep('^wgt',x=names(df.wim),perl=TRUE,value=TRUE)
    f <- formula(paste('I( (', paste(splotvars,collapse='+' ),') / (', paste(plotvars,collapse='+' ),')) ~ ts'))
    a <- xyplot(f
               ,data=df.wim
               ,main=paste("Scatterplot mean speeds from summary reports,",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
               ,strip=strip.function.a
               ,xlab="Date"
               ,ylab="Hourly mean speeds"
               ,auto.key=TRUE)
    print(a)

    dev.off()
    upload.plots.couchdb(wim.site,direction,year,imagepath,trackingdb=trackingdb)
}

#' upload plots to couchdb
#'
#' pass in a list of files, and they will be uploaded as attachments
#' to the correct document in the tracking db
#'
#' @param wim.site the WIM site id
#' @param direction the direction for this data
#' @param year
#' @param imagepath where are the images.  Will be used if you don't
#' pass in a list of files, otherwise will be ignored
#' @param trackingdb defaults to the usual 'vdsdata%2ftracking'
#' @param local defaults to TRUE, will be passed to the couchdb
#' functions.  If false it will push up to the regular couchdb, if
#' true will look for whatever is defined as local couchdb server, or
#' localhost
#' @param files.to.attach a list of files to attach to couchdb. If
#' empty or if left as default value, will look through imagepath and
#' will upload all of the files found that match the pattern for this
#' wim site id, direction, and year.
#' @return just falls of the end and exits.  La dee da.  Run this
#' strictly for the side effect of attaching files to the document in
#' the couchdb database
upload.plots.couchdb <- function(wim.site
                                 ,direction
                                 ,year
                                 ,imagepath
                                 ,trackingdb='vdsdata%2ftracking'
                                ,local=TRUE
                                ,files.to.attach=list()){

    if(length(files.to.attach) ==  0){
        file.pattern <- paste(wim.site,direction,year,'.*png',sep='_')
        file.path <- paste(paste(imagepath,direction,'/',sep=''),sep='')
        files.to.attach <- dir(file.path,
                               pattern=paste("^",file.pattern,sep=''),
                               full.names=TRUE)
    }
    for(f2a in files.to.attach){
        couch.attach(trackingdb
                    ,cdb.wimid
                    ,f2a
                    ,local=local
                     )
    }
}
