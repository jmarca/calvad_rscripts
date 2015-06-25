#' vds aggregate
#'
#' Given a dataframe, aggregate up to one hour (or whatever number of
#' seconds is passed in)
#'
#' @param df the dataframe
#' @param ts (the sequence of times)
#' @param lanes will be guessed if left blank or zero
#' @param seconds the number of seconds to aggregate up to, from 30
#'     seconds (no aggregation) to whatever
#' @return a dataframe suitable for passing to Amelia
#' @export
#'
vds.aggregate <- function(df,ts,lanes=0,seconds){
    stepsize <- 30
    ## sometimes ts isn't on 30s boundaries
    ## for example, site 318401, 2012
    ##
    ## > head(df)
    ##   nl1    ol1 nr1    or1                  ts
    ## 1   0 0.0000   0 0.0000 2012-01-01 00:01:05
    ## 2   4 0.0200   0 0.0000 2012-01-01 00:01:37
    ## 3   1 0.0056   1 0.0233 2012-01-01 00:02:07
    ## 4   1 0.0056   1 0.0044 2012-01-01 00:02:32
    ## 5   0 0.0000   4 0.0389 2012-01-01 00:03:07
    ## 6   0 0.0000   1 0.0056 2012-01-01 00:03:37

    ## fix that with round...convert time to number of seconds, divide by 30, round, multiply back up by 30:
    ##   > head(ts <- as.POSIXct(x=round(as.numeric(df_ts)/30)*30,
    ##                           tz='UTC',
    ##                           origin='1970-01-01'))
    ## [1] "2012-01-01 00:01:00 UTC" "2012-01-01 00:01:30 UTC"
    ## [3] "2012-01-01 00:02:00 UTC" "2012-01-01 00:02:30 UTC"
    ## [5] "2012-01-01 00:03:00 UTC" "2012-01-01 00:03:30 UTC"
    ##
    ts <- as.POSIXct(x=floor(as.numeric(ts)/30)*30,
                     tz='UTC',
                     origin='1970-01-01')
    attr(ts,'tzone') <- 'UTC'
    df$ts <- ts  # because I no longer need it outside the df
    df$periods <- 1

    df.mi.input <-  NA
    if(lanes==0){
        ## figure it out
        lanes <- longway.guess.lanes(df)
    }
    if(lanes>0 && difftime(ts[length(ts)],ts[1],units='hours') > 1){
        ## have lanes, have more than one hour of data (don't laugh, have
        ## a look at site 716359...3 minutes in July is all
        print(paste('in vds.processing.functions:vds.aggregate',lanes))
        n.idx <- vds.lane.numbers(lanes,c("n"))
        o.idx <- vds.lane.numbers(lanes,c("o"))
        s.idx <- vds.lane.numbers(lanes,c("s"))
        s.idx <- names(df)[is.element( names(df),s.idx)]


        ## aggregate
        ## print(paste('aggregate using sqldf'))


        len <- length(ts)

        irritating.ts <- seq(ts[1],ts[len],by=stepsize)
        ## this should be every 30s interval from start to finish times

        ts.all.df <- data.frame(ts=irritating.ts)
        mm <- merge(df,ts.all.df,all=TRUE)

        mm$timeslot <- as.POSIXct(x=floor(as.numeric(mm$ts)/seconds)*seconds,
                                  tz='UTC',
                                  origin='1970-01-01')
        attr(mm$timeslot,'tzone') <- 'UTC'

        ## print(summary(mm))

        ## do NOT include speed here
        ## all.names <- c(n.idx,o.idx,s.idx)

        all.names <- c(n.idx,o.idx)

        sqlstatement <- paste("select min(timeslot) as ts,",
                              paste ("total(",all.names,") as "
                                    ,all.names
                                    ,sep=' ',collapse=','),
                              ', total(periods) as obs_count',
                              'from mm group by timeslot'
                              )
        ## print(sqlstatement)
        ## do it
        df.agg <- sqldf::sqldf(sqlstatement,drv="RSQLite")
        ## fix time
        ## guess the units for truncate
        ## units <- 'mins'
        ## if(seconds %% 3600 == 0){
        ##     units <- 'hours'
        ## }else{
        ##     if(seconds %% 60 != 0){
        ##         units <- 'secs'
        ##     }
        ## }
        ##   print(paste('truncating time to units',units))
        ## df.agg$ts <- trunc(df.agg$ts,units=units)
        df.agg$ts <- as.POSIXct(df.agg$ts)
        attr(df.agg$ts,'tzone') <- 'UTC'

        ## print('result of aggregation')
        ## print(summary(df.agg))

        ## print(table(df.agg$obs_count))

        ## properly do the aggregation of speed, occupancy
        ## again, skip speed
        ## df.agg[,c(o.idx,s.idx)] <- df.agg[,c(o.idx,s.idx)] / df.agg[,'obs_count']
        df.agg[,c(o.idx)] <- df.agg[,c(o.idx)] / df.agg[,'obs_count']


        keep <- !is.na(df.agg$obs_count) & df.agg$obs_count == seconds/stepsize
        ## print (table(keep))

        ## this is tricky.  So because I am summing above, I only keep those
        ## time obs_count that have a full set of observations (if
        ## seconds=3600, that is 3600/30=120 observations) because
        ## obs_count==120.  Otherwise, if obs_count is less than 120, then I am
        ## not going to keep that hour of data.  A little bit wasteful of
        ## information, but the flip side is imputing every 30 seconds and
        ## that is not possible.

        clearcols <- setdiff(names(df.agg),c('ts','obs_count'))
        ## print(clearcols)
        df.agg[!keep,clearcols]<-NA

        ts.lt <- as.POSIXlt(df.agg$ts)
        df.agg$tod   <- ts.lt$hour + (ts.lt$min/60)
        df.agg$day   <- ts.lt$wday
        df.mi.input <- df.agg
    }
    df.mi.input
}

## oldway.vds.aggregate <- function(df,ts,lanes=0,seconds){
##   df.mi.input <-  NA
##   if(lanes==0){
##     ## figure it out
##     lanes <- longway.guess.lanes(df)
##   }
##   if(lanes>0 && difftime(ts[length(ts)],ts[1],units='hours') > 1){
##     ## have lanes, have more than one hour of data (don't laugh, have
##     ## a look at site 716359...3 minutes in July is all
##     print(paste('in vds.processing.functions:vds.aggregate',lanes))
##     n.idx <- vds.lane.numbers(lanes,c("n"))
##     o.idx <- vds.lane.numbers(lanes,c("o"))
##     s.idx <- vds.lane.numbers(lanes,c("s"))

##     df$periods <- 1

##     print(paste('make df.zoo.n'))
##     df.zoo.n <- zooreg(df[,c(n.idx,'periods')],order.by=ts)
##     df.zoo.n <-  aggregate(df.zoo.n,
##                            as.numeric(time(df.zoo.n)) -
##                            as.numeric(time(df.zoo.n)) %% seconds,
##                            sum, na.rm=TRUE)

##     print(paste('make df.zoo.o'))
##     df.zoo.o <- zooreg(df[,c(o.idx,'periods')],order.by=ts)
##     df.zoo.o <-  aggregate(df.zoo.o,
##                            as.numeric(time(df.zoo.o)) -
##                            as.numeric(time(df.zoo.o)) %% seconds,
##                            sum, na.rm=TRUE)
##     df.zoo.o <- df.zoo.o/df.zoo.o$periods

##     print(paste('merge sets'))
##     ## merge sets
##     aggregate.combined <- merge( df.zoo.n,df.zoo.o, suffixes = c("sum", "ave"))
##     time(aggregate.combined) <- time(df.zoo.n)

##     ## convert to ts, create "missing" hours
##     full.hours<-aggregate.combined

##     keep <- aggregate.combined[,'periods.sum']==seconds/30
##     ## this is tricky.  So because I am summing above, I only keep those
##     ## time periods that have a full set of observations (if
##     ## seconds=3600, that is 3600/30=120 observations) because
##     ## periods==120.  Otherwise, if periods is less than 120, then I am
##     ## not going to keep that hour of data.  A little bit wasteful of
##     ## information, but the flip side is imputing every 30 seconds and
##     ## that is not possible.

##     full.hours[!keep,]<-NA
##     full.hours$periods.sum <- aggregate.combined$periods.sum
##     df.mi.input <- unzoo.incantation(full.hours)
##     df.mi.input['obs_count'] <- df.mi.input['periods.sum']
##     df.mi.input['periods.sum'] <- NULL

##     ## I son't want periods.ave
##     df.mi.input['periods.ave']<- NULL

##     ## merge with the complete list of times
##     ## this makes sure all times steps have a value
##     len <- length(df.mi.input$ts)
##     irritating.ts <- seq(df.mi.input$ts[1],df.mi.input$ts[len],by=seconds)
##     ts.all.df <- data.frame(ts=irritating.ts)
##     mm <- merge(df.mi.input,ts.all.df,all=TRUE)
##     ts.lt <- as.POSIXlt(mm$ts)
##     mm$tod   <- ts.lt$hour + (ts.lt$min/60)
##     mm$day   <- ts.lt$wday
##     df.mi.input <- mm
##   }
##   df.mi.input
## }
