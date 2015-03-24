source('./utils.R')

vds.aggregate <- function(df,ts,lanes=0,seconds){
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

    df$periods <- 1

    print(paste('make df.zoo.n'))
    df.zoo.n <- zooreg(df[,c(n.idx,'periods')],order.by=ts)
    df.zoo.n <-  aggregate(df.zoo.n,
                           as.numeric(time(df.zoo.n)) -
                           as.numeric(time(df.zoo.n)) %% seconds,
                           sum, na.rm=TRUE)

    print(paste('make df.zoo.o'))
    df.zoo.o <- zooreg(df[,c(o.idx,'periods')],order.by=ts)
    df.zoo.o <-  aggregate(df.zoo.o,
                           as.numeric(time(df.zoo.o)) -
                           as.numeric(time(df.zoo.o)) %% seconds,
                           sum, na.rm=TRUE)
    df.zoo.o <- df.zoo.o/df.zoo.o$periods

    print(paste('merge sets'))
    ## merge sets
    aggregate.combined <- merge( df.zoo.n,df.zoo.o, suffixes = c("sum", "ave"))
    time(aggregate.combined) <- time(df.zoo.n)

    ## convert to ts, create "missing" hours
    full.hours<-aggregate.combined

    keep <- aggregate.combined[,'periods.sum']==seconds/30
    ## this is tricky.  So because I am summing above, I only keep those
    ## time periods that have a full set of observations (if
    ## seconds=3600, that is 3600/30=120 observations) because
    ## periods==120.  Otherwise, if periods is less than 120, then I am
    ## not going to keep that hour of data.  A little bit wasteful of
    ## information, but the flip side is imputing every 30 seconds and
    ## that is not possible.

    full.hours[!keep,]<-NA
    full.hours$periods.sum <- aggregate.combined$periods.sum
    df.mi.input <- unzoo.incantation(full.hours)
    df.mi.input['obs_count'] <- df.mi.input['periods.sum']
    df.mi.input['periods.sum'] <- NULL

    ## I son't want periods.ave
    df.mi.input['periods.ave']<- NULL

    ## merge with the complete list of times
    ## this makes sure all times steps have a value
    len <- length(df.mi.input$ts)
    irritating.ts <- seq(df.mi.input$ts[1],df.mi.input$ts[len],by=seconds)
    ts.all.df <- data.frame(ts=irritating.ts)
    mm <- merge(df.mi.input,ts.all.df,all=TRUE)
    ts.lt <- as.POSIXlt(mm$ts)
    mm$tod   <- ts.lt$hour + (ts.lt$min/60)
    mm$day   <- ts.lt$wday
    df.mi.input <- mm
  }
  df.mi.input
}
