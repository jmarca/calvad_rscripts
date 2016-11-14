

#' find wim gaps
#'
#' An alternate approach would be to check out the hourly values, if
#' there is a gap in an hour than mark the day as missing??
#'
#' no.
#'
#' The ultimate goal here (at this time) is to be able to model a
#' short term and long term time series model.  That is, to make a
#' model of truck counts over a day, and then make a model of truck
#' counts over weeks and months.
#'
#' I think that perhaps the best approach is to model interarrival
#' times as a negative poisson whose parameter varies by time of day.
#' So that is the model, a smoothly varying negative poisson
#' parameter.
#' and then for long term variations, make model weekly truck counts
#' as a time series.  But missing days are a problem, eh?  Do the same
#' thing as below, first check days and insert missing or 0, then
#' check weeks insert missing if any day inthe week is missing.
#'
#' this function is passed raw df.wim, which is probably aggregated
#' up to hourly from the individual observations.
#'
#' the problem this function solves is that missing observations in
#' an hour aren't indicative of truly missing data.  It just might
#' be the case that there weren't any trucks that hour.  However,
#' because we are counting by aggregating individual observations of
#' trucks, the aggregation step will insert NA instead of zero if
#' there aren't any vehicles observed in that time period.
#'
#' So what we do here is to aggregate the data passed to us up to
#' daily counts.  Then if there are NAs in a day, and if the
#' majority of the hours in the day are NA, then the data can be
#' left NA.  However if just one or two hours in the day are NA, the
#' probably those NAs can be recoded to be zero.
#'
#' After doing this on a few stations, now I'm not so sure.  A day
#' with a few non-missing hours might be better handled if those
#' hours were set to zero rather than imputed, but that should work
#' itself out.  Hmm, no it won't.  If unobserved should be zero but
#' is coded as missing, then there will be no hard zeros in the
#' observation set, so the imputation will be off some.
#'
#' So that is what I am after here.  Find those parts of the day
#' that are NA that should be zero, because the whole day is
#' ordinary.  Days that have really low counts if you ignore NA
#' should be left alone.
#'
#' @param df.wim the WIM data frame
#' @param count.pattern the pattern to look for count type variables, defaults to  "^(truck|heavyheavy)"
#' @return the modified dataframe, with bad sensor days left with
#' truck counts at NA, and good sensor days with no data given counts
#' of zero rather than NA
#' @export
find.wim.gaps <- function(df.wim,count.pattern = "^(truck|heavyheavy)"){

  ## First, aggregate up to a day using zoo

  ## ignore 'ts' in zoo object
  zn <- names(df.wim)[names(df.wim)!='ts']

  df.wim.zoo <- zoo::zooreg(df.wim[,zn],order.by=df.wim$ts,frequency=1/3600)

  truck.lanes <- grep( pattern="^truck",x=names(df.wim),perl=TRUE,value=TRUE)

  ## make time series of hours into days for aggregation
  ts.daily <- as.POSIXct(trunc(df.wim$ts,units='days'),tz='UTC')

  ## aggregate
  daily.aggregate <- aggregate(df.wim.zoo[,truck.lanes],ts.daily,sum,na.rm=TRUE)
  ## then sum up the trucks over lanes, if necessary
  trucks.nona <- daily.aggregate
  if(length(truck.lanes)>1){
    trucks.nona <- apply(daily.aggregate,1,sum,na.rm=TRUE)
  }
  ##  If you assume that some NA values are mistakenly encoded as
  ## such from low volume days, then the second value will necessarily
  ## have a higher distribution of counts of trucks.

  ## the idea then is to pick off those days that are
  ## probably just low volume days, rather than days in which the
  ## sensor cut out or was malfunctioning.  To do that, I set a cutoff
  ## value, and all days with more than that value are recorded as zero
  ## counts, all days with less are left as is.

  ## arbitrarily choose the 20th percentile as the cutoff .. but only
  ## of days with non-zero counts.  I'm assuming a day with zero
  ## counts is probably a bad day!


  cutoff <- quantile(trucks.nona[trucks.nona>0],c(0.2),na.rm=TRUE)

  ## trucks.na days with less than that count are bad sensor days and all left at na.
  ## trucks na days with more than that are okay sensor days and code NA to 0
  recode.these <- trucks.nona > cutoff

  ## default to false
  really.zeros <- rep(FALSE,length(ts.daily))

  ts.agg <- unclass(time(daily.aggregate))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')

  for (day in ts.agg[recode.these]){
    really.zeros[ts.daily==day] <- TRUE
  }

  count.vars <- grep( pattern=count.pattern,x=names(df.wim),perl=TRUE,value=TRUE)
  for (cv in count.vars){
    cv.zero <- really.zeros & is.na(df.wim[,cv])
    df.wim[cv.zero,cv] <- 0
  }

  ## okay, that sorted, I also want to add one more thing.  If I have
  ## good truck observations in lane 1, and NA for trucks in other
  ## lanes, then I want to set the other lanes to 0, not NA

  ## first set up the truck lane variables
  truck.vars <- grep( pattern="^truck",x=count.vars,perl=TRUE,value=TRUE)
  truck.lane.l <- grep (pattern="_l1$",x=truck.vars,perl=TRUE,value=TRUE)
  truck.lane.r <- sort(grep (pattern="_r\\d+$",x=truck.vars,perl=TRUE,value=TRUE))
  truck.vars <- c(truck.lane.r,truck.lane.l)

  if(length(truck.vars)>1){
    for(lane in 2:length(truck.vars)){
      zerocount <- !is.na(df.wim[,truck.vars[1]]) & is.na(df.wim[,truck.vars[lane]])
      ## build an index for count data for this lane
      pattern = substring(truck.vars[lane],6)[1]
      lane.count.vars <- grep (pattern=pattern,x=count.vars,perl=TRUE,value=TRUE)
      df.wim[zerocount,count.vars] <- 0
    }
  }

  df.wim

}


#' Make WIM events
#'
#' block out the good periods and bad periods in time
#'
#' Will create the events by calling \code{\link{summarize.events}}
#' first, and then save those events by calling
#' \code{\link{save.events.file}}
#'
#' @param df the WIM dataframe
#' @param year the year of data
#' @param wim.site the WIM site id
#' @param fname the file name to save data to
#' @param path the path where you want to stick fname
#' @param con a database connection to use for valid dbnames
#' @return the output of call to \code{\link{save.events.file}}
make.wim.events <- function(df,year,wim.site,fname,path,con) {

  truck.vars <- grep( pattern="^truck",x=names(df),perl=TRUE,value=TRUE)
  truck.lane.r1 <- grep (pattern="_r1$",x=truck.vars,perl=TRUE,value=TRUE)
  good.periods <- ! is.na(df[,truck.lane.r1])
  events <- summarize.events(df,year,good.periods,wim.site,df$ts,'wim')

  save.events.file(path,fname,year,events)
}



find.complete.weeks.wim.gaps <- function(df.wim){
  ## aggregate up to a week, if missing, then leave missing.  if not, then not, set to zero

  ## no op for now
  df.wim
}

#' fill WIM gaps
#'
#' run Amelia to fill in missing values in the WIM observations
#'
#' @param df.wim the WIM dataframe, typically not the raw obs but
#' rather aggregated up to one hour of observation data
#' @param count.pattern the regex to use to identify count variables
#' @param mean.pattern the regex to use to identify mean type
#' variables (not count variables, need to be handled differently by
#' Amelia).  These aren't really mean values, usually.  Rather they
#' are summed values like axle weight that in the end get divided by
#' the count to produce a mean.  But because they aren't really
#' counted values, they behave better in Amelia than pure counts.
#' @param mean.exclude.pattern the regex to use to exclude variables
#' from the Amelia run.  The default is ^mean because you really don't
#' want to include actual mean values in an amelia run
#' @param plotfile the name of a plotfile, I guess.  If not
#' emptystring (the default) then the standard Amelia plot will be run
#' and saved to this file.  If it is empty, then no plots will be run
#' @return the output of running amelia
#'
fill.wim.gaps <- function(df.wim
                         ,count.pattern='^(not_heavyheavy|heavyheavy|count_all_veh_speed)'
                         ,mean.pattern="_(weight|axle|len|speed)"
                         ,mean.exclude.pattern="^(mean)"
                         ,plotfile=""){

  ## run amelia on the wim data alone

  ##
  ## I had the idea that first I want to just impute counts, and then
  ## later I want to impute variables that depend on the counts, like
  ## weight, speed, and axles
  ##
  ic.names <- names(df.wim)

    ## attempt to fix speed imputation
    spd_cnt_names <- grep( pattern="count_all_veh_speed",x=ic.names,perl=TRUE,value=TRUE)
    spd_wgt_names <- grep( pattern="wgt_spd_all_veh_speed",x=ic.names,perl=TRUE,value=TRUE)
    df.wim[,spd_wgt_names] <- df.wim[,spd_wgt_names]/df.wim[,spd_cnt_names]


  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)

  ic.names <- grep( pattern=count.pattern,
                   x=ic.names,perl=TRUE,value=TRUE,
                   invert=TRUE)

  ## sort out the "mean variables" that are really sums at this point
  mean.vars <- grep(pattern=mean.pattern,
                    x=ic.names,
                    perl=TRUE,value=TRUE)
  ## exclude the "mean exclude" variables from mean vars
  mean.vars <- grep(pattern=mean.exclude.pattern,
                    x=mean.vars,
                    perl=TRUE,value=TRUE,
                    invert=TRUE)


  M <- 10000000000  #arbitrary bignum for max limits

  ## now build up all the final variables for the amelia call


    ic.names <- names(df.wim)

    ## generate the position of each count variable
    pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]

    ## for the count variables,
    ## make a max allowed value of 110% of observed values and a min value of 0
    max.vals <- apply( df.wim[,count.vars], 2, max ,na.rm=TRUE)
    min.vals <- apply( df.wim[,count.vars], 2, min ,na.rm=TRUE)
    pos.bds <- cbind(pos.count,min.vals,1.10*max.vals)

    ## for axles, we need to limit min of zero, max of observed?
    ## because it isn't per truck it is per sum of trucks
    pos.means <- (1:length(ic.names))[is.element(ic.names, c(mean.vars))]
    max.vals <- apply( df.wim[,mean.vars], 2, max ,na.rm=TRUE)
    min.vals <- apply( df.wim[,mean.vars], 2, min ,na.rm=TRUE)
    pos.bds <- rbind(pos.bds,cbind(pos.means,min.vals,max.vals))

    sqrt.vars <- c(count.vars,mean.vars)
    sqrt.vars <- grep(pattern='all_veh_speed',
                      x=sqrt.vars,
                      perl=TRUE,
                      value=TRUE,
                      invert=TRUE)

  print("bounds:")
  print(pos.bds)

  ## Impute some but not all of the variables.  I want to impute the
  ## mean vars, but not nasty vars, sd vars, the truck counts, and any
  ## other variables that weren't selected by the count var passed
  ## pattern.

  ## so instead of listing each, figure out which aren't

  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))

  print(paste("excluded:",  paste(exclude.as.id.vars,collapse=' ')))

  df.truckamelia.b <-
      Amelia::amelia(df.wim,
                     idvars=exclude.as.id.vars,
                     ts="tod",
                     splinetime=6,
                     lags =count.vars,
                     leads=count.vars,
                     sqrts=sqrt.vars,
                     cs="day",intercs=TRUE,
                     emburn=c(2,50),
                     bounds = pos.bds,
                     max.resample=10
                     ## ,empri = 0.05 *nrow(df.wim)
                     )

  ## make plots if requested

  if(plotfile!=''){
      print('plotting result')
      plotfile <- fixup.plotfile.name(plotfile)
      png(filename = plotfile,
          width=1600,
          height=1600,
          bg="transparent",
          pointsize=24)
      plot(df.truckamelia.b,compare=TRUE,overimpute=TRUE,ask=FALSE)
      dev.off()
  }

  df.truckamelia.b
}

#' fixup plotfile name
#'
#' This will take in a string, and mess with it to make it work right,
#' with that number pattern thing and ending in png
#'
#' @param plotfile the original name to plot to
#' @return the fixed plotfile name, suitable for general use with
#' multiple plots, etc
fixup.plotfile.name <- function(plotfile){
    if(! grepl("%03d",x=plotfile)[1]){
        ## need to add a little numbering thing
        if(! any(grepl(".png$",x=plotfile))){
            ## tack on at the end
            plotfile <- paste(plotfile,"%03d.png",sep="_")
        }else{
            ## swap in for png bit
            plotfile <- gsub("(\\.png)","_%03d\\1",plotfile)
        }
    }
    plotfile
}

##' figure the truck proportions
##'
##' this totally won't run, but I like the little lane.types hack so
##' I'm keeping it around.  What it does is if I have some count
##' variables that are trucks, like say heavy.heavy.trucks, then you
##' can also figure out not_heavy.heavy.trucks, and if you have
##' overweight trucks, you can figure out not overweight trucks, etc.
##' Not used anymore, as I didn't have any reason to track things like
##' over weight or over long trucks, etc
##' @title truck.proportions
##' @param df.wim the wim dataframe
##' @param count.vars count variables to use in the figuring
##' @return a side effect of altering the df.wim
##' @author James E. Marca
truck.proportions <- function(df.wim,count.vars){
      ## trying to switch from trucks and others to portions of trucks.  I
  ## don't rmemeber if this works well or not in the imputation.

  not.truck.vs <- grep( pattern='^truck_',x=count.vars,perl=TRUE,value=TRUE,invert=TRUE)
  ## reset each of these non-truck count values to a fraction of the
  ## truck count
  truck.vs <- grep( pattern='^truck_',x=count.vars,perl=TRUE,value=TRUE)

  new.cnt.vs <- not.truck.vs

  lane.types <- strsplit(truck.vs,"^truck_")

  for(l.type in lane.types){
    ## match corresponding lanes in not.truck.vars
    this.lane.vars <- grep(pattern=l.type[2],x=not.truck.vs,perl=TRUE,value=TRUE)
    this.truck.var <- grep(pattern=l.type[2],x=truck.vs,perl=TRUE,value=TRUE)
    for(vvar in this.lane.vars){
      new.var <- paste('not',vvar,sep='.')
      df.wim[,new.var] <- df.wim[,this.truck.var] - df.wim[,vvar]
      new.cnt.vs <- c(new.cnt.vs,new.var)
    }
  }
  df.wim
}
