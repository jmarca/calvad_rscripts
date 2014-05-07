library('Zelig')


### find wim gaps discussion:
## An alternate approach would be to check out the hourly values, if
## there is a gap in an hour than mark the day as missing??

## no.

## The ultimate goal here (at this time) is to be able to model a
## short term and long term time series model.  That is, to make a
## model of truck counts over a day, and then make a model of truck
## counts over weeks and months.

## I think that perhaps the best approach is to model interarrival
## times as a negative poisson whose parameter varies by time of day.
## So that is the model, a smoothly varying negative poisson
## parameter.

## and then for long term variations, make model weekly truck counts
## as a time series.  But missing days are a problem, eh?  Do the same
## thing as below, first check days and insert missing or 0, then
## check weeks insert missing if any day inthe week is missing.


find.wim.gaps <- function(df.wim,count.pattern = "^(truck|heavyheavy)"){

  ## this function is passed raw df.wim, which is probably aggregated
  ## up to hourly from the individual observations.

  ## the problem this function solves is that missing observations in
  ## an hour aren't indicative of truly missing data.  It just might
  ## be the case that there weren't any trucks that hour.  However,
  ## because we are counting by aggregating individual observations of
  ## trucks, the aggregation step will insert NA instead of zero if
  ## there aren't any vehicles observed in that time period.

  ## So what we do here is to aggregate the data passed to us up to
  ## daily counts.  Then if there are NAs in a day, and if the
  ## majority of the hours in the day are NA, then the data can be
  ## left NA.  However if just one or two hours in the day are NA, the
  ## probably those NAs can be recoded to be zero.

  ## After doing this on a few stations, now I'm not so sure.  A day
  ## with a few non-missing hours might be better handled if those
  ## hours were set to zero rather than imputed, but that should work
  ## itself out.  Hmm, no it won't.  If unobserved should be zero but
  ## is coded as missing, then there will be no hard zeros in the
  ## observation set, so the imputation will be off some.
  ##

  ## So that is what I am after here.  Find those parts of the day
  ## that are NA that should be zero, because the whole day is
  ## ordinary.  Days that have really low counts if you ignore NA
  ## should be left alone.

  ## First, aggregate up to a day using zoo

  ## ignore 'ts' in zoo object
  zn <- names(df.wim)[names(df.wim)!='ts']

  df.wim.zoo <- zooreg(df.wim[,zn],order.by=df.wim$ts,frequency=1/3600)

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


make.wim.events <- function(df,year,wim.site,fname,path) {

  truck.vars <- grep( pattern="^truck",x=names(df),perl=TRUE,value=TRUE)
  truck.lane.r1 <- grep (pattern="_r1$",x=truck.vars,perl=TRUE,value=TRUE)
  good.periods <- ! is.na(df[,truck.lane.r1])
  events <- summarize.events(df,year,good.periods,wim.site,df$ts,'wim')

  save.events.file(path,fname,year,events)

}


## I need to do some exploratory work in the time series classes and
## functions to see what is what.  After I do that I can tack this
## stuff.  The thing is, I don't really know if any of these trends
## are real, so I can't tackle them yet.

find.complete.weeks.wim.gaps <- function(df.wim){
  ## aggregate up to a week, if missing, then leave missing.  if not, then not, set to zero

  ## no op for now
  df.wim
}


fill.wim.gaps <- function(df.wim,count.pattern = "^(truck|heavyheavy)",mean.pattern="_(weight|axle|len|speed)", mean.exclude.pattern="^(mean)"){

  ## run amelia on the wim data alone

  ##
  ## I had the idea that first I want to just impute counts, and then
  ## later I want to impute variables that depend on the counts, like
  ## weight, speed, and axles
  ##
  ic.names <- names(df.wim)
  ## keep standard deviation data from the data
  sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
  ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)

  # want to parameterize count variables

  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)


  ## do not lag/lead ts,tod,day,id add sqrt transform to count data?
  # id.vars <-   grep( pattern="(ts|id|imp)",x=ic.names,perl=TRUE,value=TRUE)


  ## sort out the "mean variables" that are really sums at this point
  mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ## but to keep the matrix invertibe (I hope) keep all the double
  ## counting weights, speeds, lengths out (hh, lh, and over type
  ## vars)
  ## nasty.vars <- grep( pattern=mean.exclude.pattern,x=mean.vars ,perl=TRUE,value=TRUE)

  mean.vars <- grep( pattern=mean.exclude.pattern,x=mean.vars ,perl=TRUE,value=TRUE,invert=TRUE)


  M <- 10000000000  #arbitrary bignum


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

  ## now build up all the final variables for the amelia call

  ## first get all the names in the data frame
  ic.names <- names(df.wim)

  ##make sure there are not dupes

  ## the count variables
  new.cnt.vs <- intersect(new.cnt.vs,ic.names)

  ## generate the position of each count variable
  pos.count <- (1:length(ic.names))[is.element(ic.names, c(new.cnt.vs))]

  ## for the count variables,
  ## make a max allowed value of 110% of observed values and a min value of 0
  max.val <- max(df.wim[,new.cnt.vs],na.rm=TRUE)
  pos.bds <- cbind(pos.count,0,1.10*max.val)

  ## for other variables, make a looser bound of zero to M
  ## this fairly loose bound
  ## generate the position of each count variable
  pos.count <- (1:length(ic.names))[is.element(ic.names, mean.vars)]
  pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))


  ## Impute some but not all of the variables.  I want to impute the
  ## mean vars, but not nasty vars, sd vars, the truck counts, and any
  ## other variables that weren't selected by the count var passed
  ## pattern.

  ## so instead of listing each, figure out which aren't

  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,new.cnt.vs,'tod','day'))

  print(paste("excluded:",  paste(exclude.as.id.vars,collapse=' ')))



  ## df.truckamelia.b <-
  ##   amelia(df.wim,idvars=c(id.vars,sd.vars,nasty.vars,truck.count.cols),
  ##          ts="tod",splinetime=6,
  ##          lags =lag.cols,leads=lag.cols,sqrts=lag.cols,
  ##          cs="day",intercs=TRUE,emburn=c(2,50),
  ##          bounds = pos.bds, max.resample=500,empri = 0.025 *nrow(df.wim))


  df.truckamelia.b <-
    amelia(df.wim,idvars=exclude.as.id.vars,
           ts="tod",splinetime=6,
           lags =new.cnt.vs,leads=new.cnt.vs,sqrts=new.cnt.vs,
           cs="day",intercs=TRUE,emburn=c(2,200),
           bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df.wim))

  df.truckamelia.b
}
