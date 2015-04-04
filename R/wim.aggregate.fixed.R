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
    df.spd.zoo <- zoo(data.frame(wgt.spd=lane.agg[[l]][,c("veh_speed")]*lane.agg[[l]][,c("veh_count")],
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


wim.lane.and.time.aggregation <- function(lane.data){
  ## use plyr to make aggregates, then use zoo to create missing hours in sequence
  lane.data$not_heavyheavy <- !lane.data$heavyheavy
  lane.data <- wim.recode.lanes(lane.data)
  varnames <- names(lane.data)
  mean.var.names <- c('not_heavyheavy','heavyheavy',grep( pattern="^(h|n)h_",x=varnames,perl=TRUE,value=TRUE))
  lane.data$ymdh <- trunc(lane.data$ts,units='hours')
  lane.data.agg <- split(lane.data,lane.data$lane)
  for (l in names(lane.data.agg)){
    df.zoo <- zoo(lane.data.agg[[l]][,mean.var.names],
                      ,order.by=as.numeric(lane.data.agg[[l]]$ymdh))
    df.zoo <- aggregate(df.zoo, identity, sum, na.rm=TRUE )

    names(df.zoo) <- paste(names(df.zoo),l,sep='_')
    lane.data.agg[[l]] <-  df.zoo
    rm(df.zoo)
  }
  ## combine zoo aggregates, with lane data
  df.return <- lane.data.agg[[1]]
  if(length(lane.data.agg)>1){
    for(l in 2:length(lane.data.agg)){
      df.return <- merge(df.return,lane.data.agg[[l]])
    }
  }
  rm(lane.data.agg)
  df.return
}
