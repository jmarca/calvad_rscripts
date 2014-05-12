library('lattice')

pf <- function(x,y){panel.smoothScatter(x,y,nbin=c(200,200))}
day.of.week <- c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')

make.truck.plots <- function(df,year,site,dir,cdb.id,imputed=TRUE){
  varnames <-  names(df)
  ## make some diagnostic plots

  savepath <- 'images'
  if(!file.exists(savepath)){dir.create(savepath)}
  savepath <- paste(savepath,site,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}
  trucks <- 'trucks'
  if(imputed){
    trucks <- 'imputed_trucks'
  }
  file.pattern  <- paste(site,dir,year,trucks,sep='_')
  imagefilename <- paste(savepath,paste(file.pattern,'%03d.png',sep='_'),sep='/')
  png(file = imagefilename, width=900, height=600, bg="transparent",pointsize=24)

  ## heavy heavy
  main.title <- paste("Scatterplot heavy heavy duty truck counts ",year," by time of day at site",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^heavyheavy_',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,strip = strip.custom(factor.levels=day.of.week, strip.levels = TRUE )
              ,xlab="Hour of the day"
              ,ylab="HHD truck counts"
              ,panel=pf
              ,auto.key=TRUE)
  print(a)
  ## not heavy heavy
  main.title <- paste("Scatterplot not heavy heavy duty truck counts ",year," by time of day at site",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^not_heavyheavy_',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,strip = strip.custom(factor.levels=day.of.week, strip.levels = TRUE )
              ,xlab="Hour of the day"
              ,ylab="Not HHD truck counts"
              ,panel=pf
              ,auto.key=TRUE)
  print(a)

  ## heavy heavy, time
  main.title <- paste("Scatterplot heavy heavy duty truck counts ",year," over time",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^heavyheavy_',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,xlab="Date"
              ,ylab="HHD truck counts"
              ,auto.key=TRUE)
  print(a)
  ## not heavy heavy
  main.title <- paste("Scatterplot not heavy heavy duty truck counts ",year," over time",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^not_heavyheavy_',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,xlab="Date"
              ,ylab="Not HHD truck counts"
              ,auto.key=TRUE)
  print(a)

  dev.off()

  files.to.attach <- dir(savepath,pattern=paste("^",file.pattern,sep=''),full.names=TRUE)
  for(f2a in files.to.attach){
    couch.attach(trackingdb,cdb.id,f2a)
  }

}

make.truck.plots.by.lane <- function(df,year,site,dir,cdb.id,imputed=TRUE){
  varnames <-  names(df)
  ## make some diagnostic plots
  if(length(df$lane)==0){
    return()
  }
  savepath <- 'images'
  if(!file.exists(savepath)){dir.create(savepath)}
  savepath <- paste(savepath,site,sep='/')
  if(!file.exists(savepath)){dir.create(savepath)}
  trucks <- 'trucks_bylane'
  if(imputed){
    trucks <- 'imputed_trucks_bylane'
  }
  imagefilename <- paste(savepath,paste(site,dir,year,trucks,'%03d.png',sep='_'),sep='/')
  numlanes <- length(levels(as.factor(df$lane)))
  plotheight = 300 * numlanes

  strip.function.a <- strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )
  strip.function.b <- strip.custom(which.given=2,factor.levels=lane.defs[1:numlanes], strip.levels = TRUE )

  png(file = imagefilename, width=900, height=plotheight, bg="transparent",pointsize=24)

  ## heavy heavy
  main.title <- paste("Scatterplot heavy heavy duty truck counts by time of day at site",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^heavyheavy',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day + lane'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,strip = function(...){
                strip.function.a(...)
                strip.function.b(...)
              }
              ,xlab="Hour of the day"
              ,ylab="HHD truck counts"
              ,panel=pf
              ,auto.key=TRUE)
  print(a)
  ## not heavy heavy
  main.title <- paste("Scatterplot not heavy heavy duty truck counts by time of day at site",site,dir,"\nRevised method")
  if(imputed){
    main.title <- paste(main.title, "post imputation")
  }
  plotvars <- grep('^not_heavyheavy',x=varnames,perl=TRUE,value=TRUE)
  f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day + lane'))
  a <- xyplot(f
              ,data=df
              ,main=main.title
              ,strip = function(...){
                strip.function.a(...)
                strip.function.b(...)
              }
              ,xlab="Hour of the day"
              ,ylab="Not HHD truck counts"
              ,panel=pf
              ,auto.key=TRUE)
  print(a)

  dev.off()

  ## finally using the list.files function
  files.to.attach <- list.files(path=savepath
                                ,pattern=paste(site,dir,year,trucks,'*',sep='_')
                                ,full.names=TRUE)
  for(f2a in files.to.attach){
    couch.attach(trackingdb,cdb.id,f2a)
  }

}
