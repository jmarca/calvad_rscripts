##' Get TAMS dataframe from a saved RData file
##'
##' Given the TAMS site number, the year, and the direction, will get
##' the correct RData file from some directory under the path
##' parameter.  If it exists, will return the reconstituted dataframe.
##' If not, not.
##'
##' @title load.tams.from.csv
##' @param tams.site the TAMS site number
##' @param year the year
##' @param direction the direction
##' @param tams.path the root path in the file system to start looking
##'     for the RData files.  The code will add in the year, then the
##'     site number, then the direction, so that the file can be found
##'     in the expected place
##' @param filename.pattern the pattern to use when searching.  Will
##'     look for tams.agg.RData by default, but if, say, you want to
##'     load the imputation output file, then pass in "imputed.RData"
##'     or similar
##' @return a dataframe with the raw, unimputed data, or NULL
##' @author James E. Marca
##' @export
##'
load.tams.from.csv <- function(tams.site,year,direction,
                               tams.path='/data/backup/tams',
                               filename.pattern=paste(tams.site,'.*',year,'.*\\.(csv|csv.gz)$',sep=''),
                               trackingdb='vdsdata%2ftracking'){
    isa.csv <- dir(tams.path, pattern=filename.pattern,full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
    load.result <- list()
    if(length(isa.csv)>0){
        load.result <- do.call("rbind",lapply(isa.csv,readr::read_csv,progress=TRUE))
    }
    return (load.result)
}


##' recode the TAMS lanes to have the usual names
##'
##' @title tams.recode.lanes
##' @param df the TAMS dataframe
##' @return the modified data frame with the lane names changed
##' @author James E. Marca
tams.recode.lanes <- function(df){

  ##
  ## recode to be right lane (r1), right lane but one (r2), r3, ... and then
  ## left lane (l1)
  ##

  lanes <- max(df$lane)
  Y <- wim.lane.numbers(lanes) ## nothing unique to WIM
  df$lane <- as.factor(Y[df$lane])
  df
}

##' recode the TAMS lane_dir to have canonical N,S,E,W
##'
##' @title tams.recode.lane_dir
##' @param df the TAMS dataframe
##' @return the modified df with the lane_dir changed from numeric to letters
##' @author James E. Marca
tams.recode.lane_dir <- function(df){

    ##
    ## according to Andre, 1=N, 2=S, 3=E, 4=W
    ## see email reply in file lane_dir.txt
    ##
    canonical_direction <- c('N','S','E','W')


    df$lane_dir <- as.factor(canonical_direction[df$lane_dir])
    df

}

##' aggregate tams data by lane and time
##'
##' The TAMS data is per vehicle, and so each record is a vehicle and
##' says what lane it was in.  This function will aggregate up those
##' records by time and by lane, so that each record holds all of the
##' summed data for each lane, by hour.  So for example, if there is
##' one truck in l1 and two trucks in r1 in an hour, then the result
##' will have both in that hour's record.
##'
##' The lane data is captured in the variable name.  So for example,
##' the count of not_heavyheavy in lane r1 is represented by the
##' variable not_heavyheavy_r1.  The count in lane l1 is in
##' not_heavyheavy_l1.  Similar for other variables.
##'
##' Some effort is made not to drop any data you might have
##' accumulated along the way.  The variables that are summed up are
##' "captured" by the regex pattern="^(h|n)h_".  So what that means is
##' that any variable starting with hh or nh will be included.  Name
##' your variables accordingly and you should be good.  HH stands for
##' heavy heavy, and nh stands for not heavy heavy.
##'
##' This version uses sqldf, not zoo
##'
##' @title tams.lane.and.time.aggregation
##' @param lane.data the tams data that is vehicle by vehicle, lane by lane
##' @return the aggregated data as a dataframe
##' @author James E. Marca
tams.lane.and.time.aggregation <- function(lane.dir.data){
    ## to be refactored

}
##' reshape.tams.from.csv
##' Take tams raw CSV, and turn it into one tibble per direction
##'
##' Given a csv file, this function will process the CSV into one tibble per direction.
##'
##' @title reshape.tams.from.csv
##' @param tams.csv csv file read from csv, for example, by readr::read_csv
##' @param tams.path Where to save RData files, one per direction
##' @return A list of tibbles, one per direction in the original CSV data.
##' @author James E. Marca
##' @export
##'
reshape.tams.from.csv <- function(tams.csv,tams.path,year=0,trim.to.year=TRUE){
    ## prep for sqldf if you need postgresql
    # config <- rcouchutils::get.config()
    # sqldf_postgresql(config)

    tams.site <- tams.csv$detstaid[1]
    site.lanes <- max(tams.data.csv$lane)
    hour <-  3600 ## seconds per hour
    tams.csv$hourly <- as.numeric(tams.csv$timestamp_full) - as.numeric(tams.csv$timestamp_full) %% hour

    ts.ct   <- as.POSIXct(tams.csv$timestamp_full,tz='UTC')
    tams.csv$hrly <- as.numeric(trunc(ts.ct,"hours"))
    tams.csv <- tams.recode.lanes(tams.recode.lane_dir(tams.csv))
    tams.csv$any_vehicle <- 1
    tams.csv$heavyheavy <- 0
    tams.csv$not_heavyheavy <- 0
    tams.csv$heavyheavy[tams.csv$calvad_class == 'HHDT'] <- 1
    tams.csv$not_heavyheavy[tams.csv$calvad_class == 'NHHDT'] <- 1
    ##if(length(levels(tams.csv$lane_dir)) > 1)
    tams.by.dir <- split(tams.csv,tams.csv$lane_dir)


    directions <- levels(tams.csv$lane_dir)
    mean.var.names <- c('not_heavyheavy','heavyheavy')
    for(direction in directions){
        print(paste('processing',tams.site,direction))
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        tams <- tams.by.dir[[direction]]

        if(length(dim(tams)) !=  2){
            ## probably have an issue here
            print(paste("no data loaded for",tams.site,direction," ---  skipping"))
            next
        }
        ## eventuall stick preplot stuff here


        tams.data.agg <- split(tams,tams$lane)
        for (l in names(tams.data.agg)){
            temp_df <- tams.data.agg[[l]]
            sqlstatement2 <- paste("select min(timestamp_full) as ts,",
                                   paste('total(',
                                         c(mean.var.names),
                                         ') as ',
                                         paste(mean.var.names,
                                               l,
                                               sep='_'),
                                         sep=' ',
                                         collapse=', '),
                                   ', total(any_vehicle) as ',
                                   paste('n',l,sep='_'),
                                   ', max(vehicle_count) as ',
                                   paste('maxn',l,sep='_'),
                                   ', min(vehicle_count) as ',
                                   paste('minn',l,sep='_'),
                                   'from temp_df group by hrly',
                                   sep=' ',collapse=' '
                                   )
            df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")
            df_hourly$ts <- as.POSIXct(df_hourly$ts,origin = "1970-01-01", tz = "GMT")
            df_hourly$ts <- trunc(df_hourly$ts,units='hours')
            tams.data.agg[[l]] <- df_hourly
        }
        print('combine lane by lane aggregates')
        ## combine lane by lane aggregates by same hour
        df.return <- tams.data.agg[[1]]
        if(length(tams.data.agg)>1){
            for(l in 2:length(tams.data.agg)){
                df.return <- merge(df.return,tams.data.agg[[l]],all=TRUE)
            }
        }
        print(dim(df.return))
        for(col in names(df.return)){
            navalues <- is.na(df.return[,col])
            if(any(navalues)){
                df.return[navalues,col] <- 0
            }
        }
        df.return$ts <- as.POSIXct(df.return$ts)
        all.ts <- seq(min(df.return$ts),max(df.return$ts),by=hour)
        ts.all.df <- tibble::tibble(ts=all.ts)
        attr(ts.all.df$ts,'tzone') <- 'UTC'
        df.return <- merge(df.return,ts.all.df,all=TRUE)

        if(year == 0){
            about.the.middle <- floor(length(df.return$ts))
            year <- 1900 + as.POSIXlt.date(df.return$ts[about.the.middle])$year
        }

        if(trim.to.year){
            ## trim any records not in the requested year
            print(paste('trimming to year',year))
            keep.records <- (1900 + as.POSIXlt.date(df.return$ts)$year) ==  year
            ## print(summary(keep.records))
            ## print('length original')
            ## print(dim(df.return))
            ## print('length keep')
            ## print(dim(df.return[keep.records,]))
            df.return <- df.return[keep.records,]
        }

        df.return <- add.time.of.day(df.return)
        tams.by.dir[[direction]] <- df.return

        ## save tams data for next time
        filename <- paste(tams.site,direction,year,'tams.agg.RData',sep='.')
        savepath <- tams.path
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(tams.path,year,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,tams.site,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,direction,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        filepath <- paste(savepath,filename,sep='/')

        save(df.return,file=filepath,compress='xz')
        print(paste('saved',tams.site,direction,'to',filepath))

    }

    list(tams.by.dir,site.lanes)
}

##     df.tams <- load.tams.data.straight(tams.site=tams.site,year=year,con=con)
##     ## only continue if I have real data
##     if(dim(df.tams)[1]==0){
##         print(paste('problem, dim df.tams is',dim(df.tams)))
##         rcouchutils::couch.set.state(year=year,
##                                      id=paste('tams',tams.site,sep='.'),
##                                      doc=list('imputed'='no tams data in database'),
##                                      db=trackingdb)
##         return(0)
##     }

##     df.tams.speed <- get.tams.speed.from.sql(tams.site=tams.site,year=year,con=con)
##     df.tams.split <- split(df.tams, df.tams$direction)
##     df.tams.speed.split <- split(df.tams.speed, df.tams.speed$direction)


##     db_result <- get.tams.directions(tams.site=tams.site,con=con)
##     directions <- db_result$direction

##     ## global return value for the following loop
##     tams.data <- list()
##     for(direction in directions){
##         print(paste('processing direction',direction))
##         ## direction <- names(df.tams.split)[1]
##         cdb.tamsid <- paste('tams',tams.site,direction,sep='.')

##         print('basic checks')
##         if(length(df.tams.split[[direction]]$ts)<100){
##             tams.data[[direction]] <- list()
##             rcouchutils::couch.set.state(year=year,
##                                          id=cdb.tamsid,
##                                          doc=list('imputed'='less than 100 timestamps for raw data in db'),
##                                          db=trackingdb)
##             next
##         }
##         if(length(df.tams.speed.split[[direction]]$ts)<100){
##             tams.data[[direction]] <- list()
##             rcouchutils::couch.set.state(year=year,
##                                          id=cdb.tamsid,
##                                          doc=list('imputed'='less than 100 timestamps for speed data in db'),
##                                          db=trackingdb)
##             next
##         }

##         df.tams.d <- process.tams.2(df.tams.split[[direction]])
##         df.tams.s <- df.tams.speed.split[[direction]]

##         ## fix for site 16, counts of over 100,000 per hour (actually 30 million)
##         too.many <- df.tams.s$veh_count > 10000 ## 10,000 veh in 5 minutes!
##         df.tams.s <- df.tams.s[!too.many,]

##         df.tams.split[[direction]] <- NULL
##         df.tams.speed.split[[direction]] <- NULL

##         df.tams.d <- tams.additional.variables(df.tams.d)

##         ## aggregate over time
##         print(' aggregate ')
##         df.tams.dagg <- tams.lane.and.time.aggregation(df.tams.d)

##         ## ... instead aborting above.
##         ## The one such instance so far had junk measurements
##         if(length(df.tams.s)==0){
##             ## insert one dummy record per lane
##             lastlane <- max(df.tams.d$lane)
##             dummytime <- df.tams.d$ts[1]
##             df.tams.s <- data.frame(cbind(lane=c('l1',paste('r',2:lastlane,sep=''))))
##             df.tams.s$ts <- dummytime
##             df.tams.s$veh_speed <- NA
##             df.tams.s$veh_count <- NA
##         }

##         df.tams.sagg <- make.speed.aggregates(df.tams.s)

##         df.tams.d.joint <- merge(df.tams.dagg,df.tams.sagg,all=TRUE)

##         rm(df.tams.dagg, df.tams.sagg,df.tams.s,df.tams.d )

##         df.tams.d.joint <- add.time.of.day(df.tams.d.joint)

##         db.legal.names  <- gsub("\\.", "_", names(df.tams.d.joint))

##         names(df.tams.d.joint) <- db.legal.names

##         tams.data[[direction]] <- df.tams.d.joint

##         ## right here add sanity checks for data for example, if
##         ## the hourly values are super high compared to the
##         ## "usual" max value

##         ## look at the plots for TAMS 103 S 2015 for example


##         ## save tams data for next time

##         savepath <- tams.path
##         if(!file.exists(savepath)){dir.create(savepath)}
##         savepath <- paste(tams.path,year,sep='/')
##         if(!file.exists(savepath)){dir.create(savepath)}
##         savepath <- paste(savepath,tams.site,sep='/')
##         if(!file.exists(savepath)){dir.create(savepath)}
##         savepath <- paste(savepath,direction,sep='/')
##         if(!file.exists(savepath)){dir.create(savepath)}
##         filepath <- paste(savepath,'tams.agg.RData',sep='/')
##         print(filepath)

##         save(df.tams.d.joint,file=filepath,compress='xz')
##         print(paste('saved to',filepath))
##     }
##     return (tams.data)
## }
