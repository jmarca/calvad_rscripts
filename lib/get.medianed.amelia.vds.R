library('zoo')
source('./utils.R')

get.amelia.vds.file <- function(vdsid,path='/',year,server='http://calvad.ctmlabs.net',serverfile='none'){
  df.vds.agg.imputed <- list()
  files = c(serverfile)
  if(serverfile == 'none'){
    path <- toupper(path)
    amelia.dump.file <- make.amelia.output.pattern(vdsid,year)
    files <- get.filenames(server=server,base.dir=path, pattern=amelia.dump.file)
    if(length(files)==0){
      return('todo')
    }
  }
  result <- 'fail'
  attempt <- 0
  serverfile = files[1]
  while(attempt < 10){
    attempt <-  attempt + 1
    print(paste('try',attempt,'loading stored vds amelia object from file',serverfile))
    fetched <- fetch.remote.file(server,service='vdsdata',root=paste(path,'/',sep=''),file=serverfile)
    if(fetched == 'df.vds.agg.imputed') break
    print(paste('attempt ',attempt))
  }
  if(result == 'reject'){
    return(paste('rejected',files[1]))
  }
  if (attempt>9){
    stop('no data from data server')
  }
  df.vds.agg.imputed
}

get.amelia.vds.file.local <- function(vdsid,path='/',year,server,serverfile){
  df.vds.agg.imputed <- list()

  target.file <- make.amelia.output.pattern(vdsid,year)
  isa.df <- dir(path, pattern=target.file,full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
  print(paste(path,target.file,isa.df[1],sep=' : '))
  if(length(isa.df)==0){
      return('todo')
  }
  print (paste('loading', isa.df[1]))
  load.result <-  load(file=isa.df[1])
  print(load.result)
  df.vds.agg.imputed

}

unget.amelia.vds.file <- function(vdsid,path='/',year,server='http://calvad.ctmlabs.net'){

  path <- toupper(path)
  amelia.dump.file <- make.amelia.output.pattern(vdsid,year)
  files <- get.filenames(server=server,base.dir=path, pattern=amelia.dump.file)
  df.vds.agg.imputed <- list()
  if(length(files)==0){
    return()
  }
  unmap.temp.files(files[1])
  return()
}

tempfix.borkborkbork <- function(df){
  ## use zoo to combine a mean, no, median value
  varnames <- names(df)
  varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)

  df.z <- zooreg(df[,varnames]
                 ,order.by=as.numeric(df$ts))

  hour=3600
    print('make it an hour')
  df.z$tick <- 1
    df.z <-  aggregate(df.z,
                       as.numeric(time(df.z)) -
                       as.numeric(time(df.z)) %% hour,
                       sum, na.rm=TRUE)
  names.occ <- grep( pattern="(^o(l|r)\\d+$)",x=names(df.z),perl=TRUE)
  df.z[,names.occ] <-  df.z[,names.occ] / df.z[,'tick']
  df.z$tick <- NULL
  unzoo.incantation(df.z)
}

medianed.aggregate.df <- function(df.combined,op=median){
  print('use zoo to combine a value')
  varnames <- names(df.combined)
  varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)

  df.z <- zooreg(df.combined[,varnames]
                       ,order.by=as.numeric(df.combined$ts))

  ## using median not mean because median is resistant to extreme outliers
  df.z <- aggregate(df.z, identity, op, na.rm=TRUE )
  hour=3600
  print(table(df.z$tod))
  print('make it an hour')
  df.z$tick <- 1
  df.z <-  aggregate(df.z,
                     as.numeric(time(df.z)) -
                     as.numeric(time(df.z)) %% hour,
                     sum, na.rm=TRUE)
  names.occ <- grep( pattern="(^o(l|r)\\d+$)",x=names(df.z),perl=TRUE)
  df.z[,names.occ] <-  df.z[,names.occ] / df.z[,'tick']
  df.z$tick <- NULL
  df.z
}

condense.amelia.output.into.zoo <- function(df.amelia,op=median){

  ## as with the with WIM data, using median
  df.amelia.c <- df.amelia$imputations[[1]]
  if(length(df.amelia$imputations) >1){
    for(i in 2:length(df.amelia$imputations)){
      df.amelia.c <- rbind(df.amelia.c,df.amelia$imputations[[i]])
    }
  }
  df.zoo <- medianed.aggregate.df(df.amelia.c,op)
  df.zoo
}

get.zooed.vds.amelia <- function(vdsid,serverfile='none',path='none',remote=TRUE,year){
    print(paste('in get.zooed.vds.amelia, remote is',remote))
    df.vds.agg.imputed <- list()
    if(path=='none'){
        path <- district.from.vdsid(vdsid)
    }
    if(remote){
        df.vds.agg.imputed <- get.amelia.vds.file(vdsid,path=path,year=year,serverfile=serverfile)
    }else{
        print('calling local version')
        df.vds.agg.imputed <- get.amelia.vds.file.local(vdsid,path=path,year=year,serverfile=serverfile)
    }

    if(length(df.vds.agg.imputed) == 1){
    print("amelia run for vds not good")
    return(NULL)
  }else if(!length(df.vds.agg.imputed)>0 || !length(df.vds.agg.imputed$imputations)>0 || df.vds.agg.imputed$code!=1 ){
    print("amelia run for vds not good")
    return(NULL)
  }
  ## as with the with WIM data, using median
    print('stack the imputations')
  df.vds.amelia.c <- df.vds.agg.imputed$imputations[[1]]
  if(length(df.vds.agg.imputed$imputations) >1){
    for(i in 2:length(df.vds.agg.imputed$imputations)){
      df.vds.amelia.c <- rbind(df.vds.amelia.c,df.vds.agg.imputed$imputations[[i]])
    }
  }
  medianed.aggregate.df(df.vds.amelia.c)
}

get.and.plot.vds.amelia <- function(pair,year,cdb.wimid=NULL,doplots=TRUE,remote=TRUE,path){
  ## load the imputed file for this site, year
  df.vds.zoo <- get.zooed.vds.amelia(pair,year=year,path=path,remote=remote)
  ## prior to binding, weed out excessive flows
  varnames <- names(df.vds.zoo)
  flowvars <- grep('^n(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)
  for(lane in flowvars){
    idx <- df.vds.zoo[,lane] > 2500
    df.vds.zoo[idx,lane] <- 2500
  }

  couch.set.state(year,pair,doc=list('occupancy_averaged'=1))
  ts.ts <- unclass(time(df.vds.zoo))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
  ts.lt <- as.POSIXlt(ts.ts)
  df.vds.zoo$tod   <- ts.lt$hour + (ts.lt$min/60)
  df.vds.zoo$day   <- ts.lt$wday

  rm(df.vds.agg.imputed,df.vds.amelia.c)

  if(doplots){
    plot.zooed.vds.data(df.vds.zoo,pair,year)
  }
  gc()
  df.vds.zoo
}

plot.zooed.vds.data <- function(df.vds.zoo,vdsid,year,fileprefix=NULL,subhead='\npost imputation'){

  ## temporary variable for the diagnostic plots
  ## wish I could spawn this as a separate job
  df.merged <- unzoo.incantation(df.vds.zoo)
  plot.vds.data(df.merged,vdsid,year,fileprefix,subhead)
  rm(df.merged)
  gc()
  TRUE
}

check.for.plot.attachment <- function(vdsid,year,fileprefix=NULL,subhead='\npost imputation'){
  imagefileprefix <- paste(vdsid,year,sep='_')
  if(!is.null(fileprefix)){
    imagefileprefix <- paste(vdsid,year,fileprefix,sep='_')
  }
  fourthfile <- paste(imagefileprefix,'004.png',sep='_')
  print(paste('checking for ',fourthfile,'in tracking doc'))
  return (couch.has.attachment(trackingdb,vdsid,fourthfile))
}

plot.vds.data  <- function(df.merged,vdsid,year,fileprefix=NULL,subhead='\npost imputation'){
  have.plot <- check.for.plot.attachment(vdsid,year,fileprefix,subhead)
  if(have.plot){
    return (1)
  }
  print('need to make plots')
  varnames <- names(df.merged)
  ## make some diagnostic plots
  ## set up a reconfigured dataframe
  plotvars <- grep('^n(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)

  recode <- data.frame()
  for(nlane in plotvars){
    lane <- substr(nlane,2,3)
    olane <- paste('o',lane,sep='')
    keepvars <- c(nlane,olane,'tod','day','ts')
    subset <- data.frame(df.merged[,keepvars])
    subset$lane <- lane
    names(subset) <- c('n','o','tod','day','ts','lane')
    if(length(recode)==0){
      recode <- subset
    }else{
      recode <- rbind(recode,subset)
    }
  }


  numlanes <- length(levels(as.factor(recode$lane)))
  plotheight = 300 * numlanes

  savepath <- 'images'
  if(!file.exists(savepath)){dir.create(savepath)}
  savepath <- paste(savepath,vdsid,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}

  imagefileprefix <- paste(vdsid,year,sep='_')
  if(!is.null(fileprefix)){
    imagefileprefix <- paste(vdsid,year,fileprefix,sep='_')
  }

  imagefilename <- paste(savepath,paste(imagefileprefix,'%03d.png',sep='_'),sep='/')

  print(paste('plotting to',imagefilename))

  png(file = imagefilename, width=900, height=plotheight, bg="transparent",pointsize=24)

  plotvars <- grep('^n',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste( paste(plotvars,collapse='+' ),' ~ tod | day'))
  strip.function.a <- strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )
  strip.function.b <- strip.custom(which.given=2,factor.levels=lane.defs[1:length(plotvars)], strip.levels = TRUE )
  a <- xyplot( n ~ tod | day + lane, data=recode
              ,main=paste("Scatterplot volume in each lane, by time of day and day of week, for ",year," at site",vdsid,subhead),
              ,strip = function(...){
                strip.function.a(...)
                strip.function.b(...)
              }
              ,ylab=list(label='hourly volume, by lane', cex=2)
              ,xlab=list(label='time of day', cex=2)
              ,type='p' ## or 'l
              ,pch='*'
              ,scales=list(cex=1.8)
              ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
              ,panel=pf
              ,auto.key = list(
                 space='bottom',
                 points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
                 )
              )
  print(a)

  plotvars <- grep('^o(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste( paste(plotvars,collapse='+' ),' ~ tod | day'))
  a <- xyplot( o ~ tod | day + lane, data=recode
              ,main=paste("Scatterplot occupancy in each lane, by time of day and day of week, for ",year," at site",vdsid,subhead)
              ,strip = function(...){
                strip.function.a(...)
                strip.function.b(...)
              }
              ,ylab=list(label='hourly occupancy, by lane', cex=2)
              ,xlab=list(label='time of day', cex=2)
              ,type='p' ## or 'l'
              ,pch='*'
              ,scales=list(cex=1.8)
              ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
              ,panel=pf
              ,auto.key = list(
                 space='bottom',
                 points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
                 )
              )

  print(a)


  a <- xyplot(n ~ o | day + lane, data=recode
              ,main=paste("Scatterplot hourly volume vs occupancy per lane, by day of week, for ",year," at site",vdsid,subhead)
              ,strip = function(...){
                strip.function.a(...)
                strip.function.b(...)
              }
              ,ylab=list(label='hourly volume', cex=2)
              ,xlab=list(label='hourly occupancy', cex=2)
              ,type='p' ## or 'l'
              ,pch='*'
              ,scales=list(cex=1.8)
              ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
              ,panel=pf
              ,auto.key = list(
                 space='bottom',
                 points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
                 )
              )

  print(a)

  strip.function.b <- strip.custom(which.given=1,factor.levels=lane.defs[1:length(plotvars)], strip.levels = TRUE )

  a <- xyplot(n ~ ts | lane, data=recode
              ,main=paste("Scatterplot hourly volume vs time, per lane, for ",year," at site",vdsid,subhead)
              ,strip = function(...){
                strip.function.b(...)
              }
              ,ylab=list(label='hourly volume', cex=2)
              ,xlab=list(label='date', cex=2)
              ,type='p' ## or 'l'
              ,scales=list(cex=1.8)
              ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
              ,auto.key = list(
                 space='bottom',
                 points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
                 )
              )
  print(a)

  dev.off()

  files.to.attach <- dir(savepath,pattern=paste(imagefileprefix,'0',sep='_'),full.names=TRUE)
  for(f2a in files.to.attach){
    couch.attach(trackingdb,vdsid,f2a)
  }
  rm(recode)
  gc()
  TRUE
}
