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
  df.spd$ymdh <- trunc(df.spd$ts,units='hours')
  lane.agg <- split(df.spd,df.spd$lane)
  for(l in names(lane.agg)){
    if(dim(lane.agg[[l]])[1]==0){
      ## dummy <- df.spd[1,]
      ## dummy$veh_speed <- NA
      ## dummy$veh_count <- NA
      ## lane.agg[[l]] <- dummy

      ## I tried the above, but it is going to royal mess up the imputations down
      ## the road with all those NA values.  Instead, just skip it
      lane.agg[[l]] <- NULL
      next;
    }
    df.spd.zoo <- zoo::zoo(data.frame(wgt.spd=lane.agg[[l]][,c("veh_speed")]*lane.agg[[l]][,c("veh_count")],
                                 count  =lane.agg[[l]][,c("veh_count")] )
                      ,order.by=as.numeric(lane.agg[[l]]$ymdh))
    df.spd.zoo <- aggregate(df.spd.zoo, identity, sum, na.rm=TRUE )

    y.suffix <- paste('all.veh.speed',l,sep='_')
    names(df.spd.zoo) <- paste(names(df.spd.zoo),y.suffix,sep='_')
    lane.agg[[l]] <-  df.spd.zoo
    rm(df.spd.zoo)
  }
  ## combine zoo aggregates, with lane data
  df.return <- lane.agg[[1]]
  if(length(lane.agg)>1){
    for(l in 2:length(lane.agg)){
      df.return <- merge(df.return,lane.agg[[l]])
    }
  }
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
  lane.data$hourly <- as.numeric(lane.data$ts) - as.numeric(lane.data$ts) %% hour

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
      lane.data.agg[[l]] <- df_hourly
  }
  ## combine lane by lane aggregates by same hour
  df.return <- lane.data.agg[[1]]
  if(length(lane.data.agg)>1){
    for(l in 2:length(lane.data.agg)){
      df.return <- merge(df.return,lane.data.agg[[l]],all=TRUE)
    }
  }
  rm(lane.data.agg)
  df.return
}
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
