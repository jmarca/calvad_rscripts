##' Get the WIM data from the database
##'
##' I used to get WIM data from the database, then save to an RData
##' file, then use that.  But then small issues would creep in for
##' some reason.  Therefore, the default and usual way is to load WIM
##' data directly from the DB every time.  It takes longer, but leads
##' to fewer errors.
##' @title load.wim.from.db
##' @param wim.site the WIM site number
##' @param year the year
##' @param con database connection
##' @param wim.path the root path in the file system to start looking
##'     for the RData files.  The code will add in the year, then the
##'     site number, then the direction, so that the file can be found
##'     in the expected place
##' @param trackingdb the couchdb tracking db to use, defaults to
##'     'vdsdata\%2ftracking'
##' @return a dataframe with the raw, unimputed data, or NULL
##' @author James E. Marca
##' @export
load.wim.from.db <- function(wim.site,year,con,
                             wim.path='/data/backup/wim',
                             trackingdb='vdsdata%2ftracking'){

    df.wim <- load.wim.data.straight(wim.site=wim.site,year=year,con=con)
    ## only continue if I have real data
    if(dim(df.wim)[1]==0){
        print(paste('problem, dim df.wim is',dim(df.wim)))
        rcouchutils::couch.set.state(year=year,
                                     id=paste('wim',wim.site,sep='.'),
                                     doc=list('imputed'='no wim data in database'),
                                     db=trackingdb)
        return(0)
    }

    df.wim.speed <- get.wim.speed.from.sql(wim.site=wim.site,year=year,con=con)
    df.wim.split <- split(df.wim, df.wim$direction)
    df.wim.speed.split <- split(df.wim.speed, df.wim.speed$direction)


    db_result <- get.wim.directions(wim.site=wim.site,con=con)
    directions <- db_result$direction

    ## global return value for the following loop
    wim.data <- list()
    for(direction in directions){
        print(paste('processing direction',direction))
        ## direction <- names(df.wim.split)[1]
        cdb.wimid <- paste('wim',wim.site,direction,sep='.')

        print('basic checks')
        if(length(df.wim.split[[direction]]$ts)<100){
            wim.data[[direction]] <- list()
            rcouchutils::couch.set.state(year=year,
                                         id=cdb.wimid,
                                         doc=list('imputed'='less than 100 timestamps for raw data in db'),
                                         db=trackingdb)
            next
        }
        if(length(df.wim.speed.split[[direction]]$ts)<100){
            wim.data[[direction]] <- list()
            rcouchutils::couch.set.state(year=year,
                                         id=cdb.wimid,
                                         doc=list('imputed'='less than 100 timestamps for speed data in db'),
                                         db=trackingdb)
            next
        }

        df.wim.d <- process.wim.2(df.wim.split[[direction]])
        df.wim.s <- df.wim.speed.split[[direction]]

        ## fix for site 16, counts of over 100,000 per hour (actually 30 million)
        too.many <- df.wim.s$veh_count > 10000 ## 10,000 veh in 5 minutes!
        df.wim.s <- df.wim.s[!too.many,]

        df.wim.split[[direction]] <- NULL
        df.wim.speed.split[[direction]] <- NULL

        df.wim.d <- wim.additional.variables(df.wim.d)

        ## aggregate over time
        print(' aggregate ')
        df.wim.dagg <- wim.lane.and.time.aggregation(df.wim.d)

        ## ... instead aborting above.
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

        df.wim.d.joint <- merge(df.wim.dagg,df.wim.sagg,all=TRUE)

        rm(df.wim.dagg, df.wim.sagg,df.wim.s,df.wim.d )

        df.wim.d.joint <- add.time.of.day(df.wim.d.joint)

        db.legal.names  <- gsub("\\.", "_", names(df.wim.d.joint))

        names(df.wim.d.joint) <- db.legal.names

        wim.data[[direction]] <- df.wim.d.joint

        ## right here add sanity checks for data for example, if
        ## the hourly values are super high compared to the
        ## "usual" max value

        ## look at the plots for WIM 103 S 2015 for example


        ## save wim data for next time

        savepath <- wim.path
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(wim.path,year,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,wim.site,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,direction,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        filepath <- paste(savepath,'wim.agg.RData',sep='/')
        print(filepath)

        save(df.wim.d.joint,file=filepath,compress='xz')
        print(paste('saved to',filepath))
    }
    return (wim.data)
}
