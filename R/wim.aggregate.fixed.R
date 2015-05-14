##' Make speed aggregates for WIM data
##'
##' need to revise to use sqldf
##'
##' @title makes.speed.aggregates
##' @param df.spd the speed data frame from the WIM summary reports
##' @param seconds how many seconds to aggregate to
##' @return the aggregated speed data
##' @author James E. Marca
make.speed.aggregates <- function(df.spd,seconds){
    hour <-  3600 ## seconds per hour
    df.spd$hourly <- trunc(df.spd$ts,units='hours')
    ## leaves seconds hanging
    ##    as.numeric(df.spd$ts) - as.numeric(df.spd$ts) %% hour
    lane.agg <- split(df.spd,df.spd$lane)
    for(l in names(lane.agg)){
        if(dim(lane.agg[[l]])[1]==0){
            ## dummy <- df.spd[1,]
            ## dummy$veh_speed <- NA
            ## dummy$veh_count <- NA
            ## lane.agg[[l]] <- dummy

            ## I tried the above, but it is going to royal mess up the
            ## imputations down the road with all those NA values.
            ## Instead, just skip it
            lane.agg[[l]] <- NULL
            next;
        }
        temp_df <- lane.agg[[l]]
        y.suffix <- paste('all_veh_speed',l,sep='_')
        sqlstatement <- paste("select min(ts) as ts",
                              ",total( veh_speed * veh_count ) as",
                              paste("wgt_spd",y.suffix,sep='_'),
                              ",total( veh_count ) as",
                              paste("count",y.suffix,sep='_'),
                              "from temp_df group by hourly",
                              sep=' ',collapse=' ')
        df_hourly <- sqldf::sqldf(sqlstatement,drv="SQLite")
        attr(df_hourly$ts,'tzone') <- 'UTC'
        df_hourly$ts <- trunc(df_hourly$ts,units='hours')
        df_hourly$ts <- as.POSIXct(df_hourly$ts)

        lane.agg[[l]] <- df_hourly

    }
    ## combine zoo aggregates, with lane data
    df.return <- lane.agg[[1]]
    if(length(lane.agg)>1){
        for(l in 2:length(lane.agg)){
            df.return <- merge(df.return,lane.agg[[l]],all=TRUE)
        }
    }
    ## fix NA values from aggregation...should be zero count
    count.vars <- grep(pattern="^count_all_veh_speed",
                       x=names(df.return),
                       perl=TRUE,value=TRUE)
    for(count_col in count.vars){
        navalues <- is.na(df.return[,count_col])
        if(any(navalues)){
            df.return[navalues,count_col] <- 0
        }
    }

    df.return$ts <- as.POSIXct(df.return$ts)

    ## fill in missing times from min to max
    all.ts <- seq(min(df.return$ts),max(df.return$ts),by=hour)

    ts.all.df <- data.frame(ts=all.ts)
    attr(ts.all.df$ts,'tzone') <- 'UTC'
    df.return <- merge(df.return,ts.all.df,all=TRUE)

    rm(lane.agg)
    df.return
}

##' aggregate wim data by lane and time
##'
##' The WIM data is per vehicle, and so each record is a vehicle and
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
##' @title wim.lane.and.time.aggregation
##' @param lane.data the wim data that is vehicle by vehicle, lane by lane
##' @return the aggregated data as a dataframe
##' @author James E. Marca
wim.lane.and.time.aggregation <- function(lane.data){
    ## use sqldf to make aggregates
    lane.data$not_heavyheavy <- !lane.data$heavyheavy
    lane.data <- wim.recode.lanes(lane.data)
    varnames <- names(lane.data)
    mean.var.names <- c('not_heavyheavy','heavyheavy',grep( pattern="^(h|n)h_",x=varnames,perl=TRUE,value=TRUE))


    hour <-  3600 ## seconds per hour
    lane.data$hourly <- trunc(lane.data$ts,units='hours')
    ## <- as.numeric(lane.data$ts) - as.numeric(lane.data$ts) %% hour

    lane.data.agg <- split(lane.data,lane.data$lane)
    for (l in names(lane.data.agg)){
        temp_df <- lane.data.agg[[l]]
        sqlstatement2 <- paste("select min(ts) as ts,",
                               paste('total(',
                                     c(mean.var.names),
                                     ') as ',
                                     paste(mean.var.names,
                                           l,
                                           sep='_'),
                                     sep=' ',
                                     collapse=', '),
                               'from temp_df group by hourly',
                               sep=' ',collapse=' '
                               )
        df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")
        attr(df_hourly$ts,'tzone') <- 'UTC'
        df_hourly$ts <- trunc(df_hourly$ts,units='hours')
        df_hourly$ts <- as.POSIXct(df_hourly$ts)
        lane.data.agg[[l]] <- df_hourly
    }
    ## combine lane by lane aggregates by same hour
    df.return <- lane.data.agg[[1]]
    if(length(lane.data.agg)>1){
        for(l in 2:length(lane.data.agg)){
            df.return <- merge(df.return,lane.data.agg[[l]],all=TRUE)
        }
    }
    ## make NA values zero instead
    for(col in names(df.return)){
        navalues <- is.na(df.return[,col])
        if(any(navalues)){
            df.return[navalues,col] <- 0
        }
    }

    df.return$ts <- as.POSIXct(df.return$ts)

    ## fill in missing times from min to max
    all.ts <- seq(min(df.return$ts),max(df.return$ts),by=hour)

    ts.all.df <- data.frame(ts=all.ts)
    attr(ts.all.df$ts,'tzone') <- 'UTC'
    df.return <- merge(df.return,ts.all.df,all=TRUE)

    rm(lane.data.agg)
    df.return
}

## old way using zoo
## wim.lane.and.time.aggregation <- function(lane.data){
##   ## use sqldf to make aggregates
##   lane.data$not_heavyheavy <- !lane.data$heavyheavy
##   lane.data <- wim.recode.lanes(lane.data)
##   varnames <- names(lane.data)
##   mean.var.names <- c('not_heavyheavy','heavyheavy',grep( pattern="^(h|n)h_",x=varnames,perl=TRUE,value=TRUE))
##   lane.data$ymdh <- trunc(lane.data$ts,units='hours')
##   lane.data.agg <- split(lane.data,lane.data$lane)
##   for (l in names(lane.data.agg)){
##     df.zoo <- zoo::zoo(lane.data.agg[[l]][,mean.var.names],
##                       ,order.by=as.numeric(lane.data.agg[[l]]$ymdh))
##     df.zoo <- aggregate(df.zoo, identity, sum, na.rm=TRUE )

##     names(df.zoo) <- paste(names(df.zoo),l,sep='_')
##     lane.data.agg[[l]] <-  df.zoo
##     rm(df.zoo)
##   }
##   ## combine zoo aggregates, with lane data
##   df.return <- lane.data.agg[[1]]
##   if(length(lane.data.agg)>1){
##     for(l in 2:length(lane.data.agg)){
##       df.return <- merge(df.return,lane.data.agg[[l]])
##     }
##   }
##   rm(lane.data.agg)
##   df.return
## }

##' wim medianed aggregate df
##'
##' Generate a hourly aggregate of the impuation results
##'
##' @title wim.medianed.aggregate.df
##' @param df_combined the mulitple imputations, rbind into a single dataframe
##' @param op defaults to median, but you can also try mean.  This is
##' how the multiple imputations are merged into a single imputation
##' result
##'
##' @return a dataframe holding one entry per hour, with the multiple
##' imputations aggregated according to \code{op} according to whatever
##' time step the amelia run was done, and then aggregated up to one
##' hour by summing the counts and averaging the occupancies
##'
##' @author James E. Marca
wim.medianed.aggregate.df <- function(df_combined,op=median){
    if(class(df_combined) == 'amelia'){
        ## fix that
        aout <- df_combined
        df_combined <- NULL
        for(i in 1:length(aout$imputations)){
            df_combined <- rbind(df_combined,aout$imputations[[i]])
        }
    }
    varnames <- names(df_combined)
    varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,invert=TRUE,value=TRUE)
    varnames <- setdiff(varnames,c('tod','day'))

    ## use sqldf...so much faster than zoo, aggregate

    sqlstatement <- paste("select ts,",
                          paste('median(',varnames,') as ',varnames,sep=' ',collapse=','),
                          'from df_combined group by ts',
                          sep=' ',collapse=' '
                          )

    ## aggregate the multiple imputations, resulting in one value per
    ## time step
    ## print(sqlstatement)
    temp_df <- sqldf::sqldf(sqlstatement,drv="SQLite")
    attr(temp_df$ts,'tzone') <- 'UTC'

    ## at the moment this is a no-op, because I am imputing WIM data
    ## at one hour.  But keep it because it might be useful in the
    ## future
    hour <-  3600 ## seconds per hour
    temp_df$hourly <- trunc(temp_df$ts,units='hours')
    ##  <- as.numeric(temp_df$ts) - as.numeric(temp_df$ts) %% hour

    temp_df$tick <- 1 ## a value to sum up # of records per hour, to
                      ## compute averages of occupancy (because summed
                      ## occupancy is meaningless!)

    sqlstatement2 <- paste("select min(ts) as ts,",
                           paste('total(',c(varnames,'tick'),') as ',c(varnames,'tick'),sep=' ',collapse=','),
                           'from temp_df group by hourly',
                           sep=' ',collapse=' '
                           )
    ## print(sqlstatement2)
    ## generate the hourly summation
    df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")

    ## assign the correct timezone again
    attr(df_hourly$ts,'tzone') <- 'UTC'

    df_hourly$tick <- NULL

    ts.lt <- as.POSIXlt(df_hourly$ts)
    df_hourly$tod   <- ts.lt$hour + (ts.lt$min/60)
    df_hourly$day   <- ts.lt$wday

    ## all done, return value
    df_hourly
}
