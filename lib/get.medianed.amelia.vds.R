source('./utils.R')

#' get amelia vds file
#'
#' go grab the output of amelia for this detector from the file server
#'
#' currently a little out of date, review the code before using.
#' Also, obviously, it needs a server running at the other end
#'
#' @param vdsid the VDS id
#' @param path the root path to ping the web service.  Use it if there
#' is some sort of path like, say, /vile/file/server for the thing
#' @param year
#' @param server defaults to http://lysithia.its.uci.edu
#' @param serverfile the already cached server file, if any.  If
#' 'none' (the default) then an initial hit to the server will be done
#' to find the file's REST path, and also ascertain whether that file
#' exists.
#' @return either 'todo' indicating that the file is not on the
#' server, 'rejected' and some files if the remote fetch failed, or a
#' dataframe containing the amelia output
get.amelia.vds.file <- function(vdsid,path='/',year,server='http://lysithia.its.uci.edu'
                               ,serverfile='none'){
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

#' get Amelia VDS file from the local file system
#'
#' Rather than hitting a remote server, just get the Amelia output
#' from the local file system.  Use this if everything is running on
#' one machine, ya?
#'
#' @param vdsid the VDS id
#' @param path the root path of where the Amelia files are stashed
#' @param year the year of the data
#' @param server yeah well, cruft
#' @param serverfile more cruft The thing is, you can just slot in
#' this file in place of the other one above and use the same function
#' call, just these last two parameters will get ignored.
#' @return either 'todo' indicating that the file is not on this
#' machine or not yet done, or a dataframe containing the amelia
#' output
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

#' unget Amelia VDS output file
#'
#' So you need this because you can get lots of files laying around
#' filling up your hard drive.  This call will unmap any temp files
#' generated in the prior call
#'
#' @param vdsid the VDS id
#' @param path the root path to ping the web service.  Use it if there
#' is some sort of path like, say, /vile/file/server for the thing
#' @param year
#' @param server defaults to http://lysithia.its.uci.edu
#' @return nothing at all
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

## tempfix.borkborkbork <- function(df){
##   summing.zoo.combiner(df)
## }
## summing.zoo.combiner <- function(df){
##   ## use zoo to combine a mean of occupancy, sums of volumes
##   varnames <- names(df)
##   varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)

##   df.z <- zooreg(df[,varnames]
##                  ,order.by=as.numeric(df$ts))

##   hour=3600
##     print('make it an hour')
##   df.z$tick <- 1
##     df.z <-  aggregate(df.z,
##                        as.numeric(time(df.z)) -
##                        as.numeric(time(df.z)) %% hour,
##                        sum, na.rm=TRUE)
##   names.occ <- grep( pattern="(^o(l|r)\\d+$)",x=names(df.z),perl=TRUE)
##   df.z[,names.occ] <-  df.z[,names.occ] / df.z[,'tick']
##   df.z$tick <- NULL
##   unzoo.incantation(df.z)
## }

#' medianed aggregate df  Generate a hourly aggregate of the impuation results
#'
#' @param df_combined the mulitple imputations, rbind into a single dataframe
#' @param op defaults to median, but you can also try mean.  This is
#' how the multiple imputations are merged into a single imputation
#' result
#'
#' @return a dataframe holding one entry per hour, with the multiple
#' imputations aggregated according to \code{op} according to whatever
#' time step the amelia run was done, and then aggregated up to one
#' hour by summing the counts and averaging the occupancies
#'
#' Speeds are ignored because speed never works out so far
medianed.aggregate.df <- function(df_combined,op=median){
    print(paste('use sqldf to aggregate imputations'))

    varnames <- names(df_combined)
    varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)
    varnames <- setdiff(varnames,c('tod','day'))

    n.names <- grep(pattern="^n(l|r)\\d+",x=varnames,perl=TRUE,value=TRUE)
    o.names <- grep(pattern="^o(l|r)\\d+",x=varnames,perl=TRUE,value=TRUE)

    other.names <- grep(pattern="^(n|o)(l|r)\\d+",x=varnames,inv=TRUE,perl=TRUE,value=TRUE)

    ## use sqldf...so much faster than zoo, aggregate

    sqlstatement <- paste("select ts,",
                          paste('median(',varnames,') as ',varnames,sep=' ',collapse=','),
                          'from df_combined group by ts',
                          sep=' ',collapse=' '
                          )

    ## aggregate the multiple imputations, resulting in one value per
    ## time step
    print(sqlstatement)
    temp_df <- sqldf::sqldf(sqlstatement,drv="SQLite")
    attr(temp_df$ts,'tzone') <- 'UTC'


    ## aggregate up to one hour
    hour <-  3600 ## seconds per hour
    temp_df$hourly <- as.numeric(temp_df$ts) - as.numeric(temp_df$ts) %% hour

    temp_df$tick <- 1 ## a value to sum up # of records per hour, to
                      ## compute averages of occupancy (because summed
                      ## occupancy is meaningless!)

    sqlstatement2 <- paste("select min(ts) as ts,",
                           paste('total(',c(varnames,'tick'),') as ',c(varnames,'tick'),sep=' ',collapse=','),
                           'from temp_df group by hourly',
                           sep=' ',collapse=' '
                           )
    print(sqlstatement2)
    ## generate the hourly summation
    df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")

    ## divide out the number of intervals summed to create average
    ## occupancy per hour

    df_hourly[,o.names] <- df_hourly[,o.names]/df_hourly[,'tick']

    ## assign the correct timezone again
    attr(df_hourly$ts,'tzone') <- 'UTC'

    df_hourly$tick <- NULL

    ts.lt <- as.POSIXlt(df_hourly$ts)
    df_hourly$tod   <- ts.lt$hour + (ts.lt$min/60)
    df_hourly$day   <- ts.lt$wday

    ## all done, return value
    df_hourly
}

## medianed.aggregate.df.oldway <- function(df.combined,op=median){
##   print('use zoo to combine a value')
##   varnames <- names(df.combined)
##   varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)

##   df.z <- zooreg(df.combined[,varnames]
##                        ,order.by=as.numeric(df.combined$ts))

##   ## using median not mean because median is resistant to extreme outliers
##   df.z <- aggregate(df.z, identity, op, na.rm=TRUE )
##   hour=3600
##   print(table(df.z$tod))
##   print('make it an hour')
##   df.z$tick <- 1
##   df.z <-  aggregate(df.z,
##                      as.numeric(time(df.z)) -
##                      as.numeric(time(df.z)) %% hour,
##                      sum, na.rm=TRUE)
##   names.occ <- grep( pattern="(^o(l|r)\\d+$)",x=names(df.z),perl=TRUE)

##   df.z[,names.occ] <-  df.z[,names.occ] / df.z[,'tick']
##   df.z$tick <- NULL
##   df.z
## }

#' Condense Amelia output
#'
#' This combines the "imputations" part of the Amelia output object
#' into a single dataframe, with all the data first rbind-ed together,
#' and then combined into a single record per timestamp using whatever
#' op requested (defaults to median, which is resistant to possible
#' outliers that occasionally result from the imputation process)
#'
#' @param aout the Amelia output
#' @param op usually median or mean.  With the new way, it is pretty
#' much ignored for now and you're stuck with median.
#' @return a dataframe containing the aggregated results of the Amelia
#' imputations, combined one per timestamp with median.
condense.amelia.output <- function(aout,op=median){
    ## as with the with WIM data, using median

    df.c <- NULL
    for(i in 1:length(aout$imputations)){
        df.c <- rbind(df.c,aout$imputations[[i]])
    }

    df.agg <- medianed.aggregate.df(df.c,op)
    df.agg
}
## alias for backwards

#' Condense Amelia output
#'
#' old way of calling condense.amelia.output
#'
#' This combines the "imputations" part of the Amelia output object
#' into a single dataframe, with all the data first rbind-ed together,
#' and then combined into a single record per timestamp using whatever
#' op requested (defaults to median, which is resistant to possible
#' outliers that occasionally result from the imputation process)
#'
#' @param aout the Amelia output
#' @param op usually median or mean.  With the new way, it is pretty
#' much ignored for now and you're stuck with median.
#' @return a dataframe containing the aggregated results of the Amelia
#' imputations, combined one per timestamp with median.
condense.amelia.output.into.zoo <- function(aout,op){
    print('old way of accessing condense.amelia.output')
    condense.amelia.output(aout,opp)
}

#' Get aggregated VDS Amelia file
#'
#' Yet another way to call the above stack!
#'
#' This is a bit of a wrapper.  It will either hit the local or the
#' remote server to get the raw Amelia output, and then will run that
#' output through the aggregation function above, producing a
#' dataframe with just one observation/impuation per timestamp
#'
#' @param vdsid the VDS id
#' @param serverfile a cached list of the server's files
#' @param path either 'none', the remote server's service path, or the
#' local file system's root directory for Amelia output
#' @param remote default is TRUE, meaning hit a remote server, or
#' FALSE meaning hit the local server
#' @param server defaults to http://lysithia.its.uci.edu
#' @param year
#' @return the condensed, aggregated Amelia imputation results as a
#' dataframe
get.aggregated.vds.amelia <- function(vdsid,
                                      serverfile='none',
                                      path='none',
                                      remote=TRUE,
                                      server='http://lysithia.its.uci.edu',
                                      year){
    print(paste('in get.aggregated.vds.amelia, remote is',remote))
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
    condense.amelia.output(df.vds.agg.imputed)
}


#' Get and plot VDS Amelia file
#'
#' Same as call to get.aggregated.vds.amelia, but then it will plot
#' the output results.
#'
#' Which now that I think about it is sort of stupid, because really I
#' probably also want to be able to trigger the native Amelia object
#' plot functions.
#'
#' @param pair the VDS id or the wim id or the vds wim pair, I guess
#' @param year
#' @param doplots default is TRUE, set to FALSE if you want to skip
#' plotting, but then, why are you running this if you don't want
#' plots.
#' @param remote default is TRUE, meaning hit a remote server, or
#' FALSE meaning hit the local server
#' @param server defaults to http://lysithia.its.uci.edu
#' @param path either 'none', the remote server's service path, or the
#' local file system's root directory for Amelia output
#' @param force.plot defaults to FALSE, if TRUE will replot even if
#' the plot appears to exist already
#' @param trackingdb the CouchDB database that is being used to track
#' detector status.  Will push the generated plot files to this
#' database as attached files
#' @return the condensed, aggregated Amelia imputation results as a
#' dataframe
#'
#' And plots get made and saved
#'
get.and.plot.vds.amelia <- function(pair,year,doplots=TRUE,
                                    remote=TRUE,
                                    server='http://lysithia.its.uci.edu',
                                    path,
                                    force.plot=FALSE,
                                    trackingdb='vdsdata%2ftracking'){
    ## load the imputed file for this site, year
    aout <- NULL
    if(remote){
        aout <- get.amelia.vds.file(vdsid=pair,year=year,path=path,server=server)
    }else{
        aout <- get.amelia.vds.file.local(vdsid=pair,year=year,path=path)
    }
    if(is.null(aout)){
        return (NULL)
    }

    if(doplots){

        ## do the amelia default plot functions
        trigger.amelia.plots(aout,pair,year,force.plot=force.plot)
    }

    df.vds.agg <- condense.amelia.output(aout)

    ## prior to binding, weed out excessive flows
    varnames <- names(df.vds.agg)
    flowvars <- grep('^n(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)
    for(lane in flowvars){
        idx <- df.vds.agg[,lane] > 2500
        if(length(idx[idx])>0){
            df.vds.agg[idx,lane] <- 2500
            print(paste('Hey, got excessive flows for ',pair,year))
        }
    }

    ## cruft, but may as well keep it up
    couch.set.state(year,pair,doc=list('occupancy_averaged'=1))

    if(doplots){
        attach.files <- plot.vds.data(df.vds.agg,pair,year,
                                      force.plot=force.plot,
                                      trackingdb=trackingdb)
        for(f2a in files.to.attach){
            couch.attach(trackingdb,vdsid,f2a)
        }
    }
    df.vds.agg
}

## plot.zooed.vds.data <- function(df.vds.zoo,vdsid,year,fileprefix=NULL,subhead='\npost imputation',force.plot=FALSE){

##   ## temporary variable for the diagnostic plots
##   ## wish I could spawn this as a separate job
##   df.merged <- unzoo.incantation(df.vds.zoo)
##   plot.vds.data(df.merged,vdsid,year,fileprefix,subhead,force.plot=force.plot)
##   rm(df.merged)
##   gc()
##   TRUE
## }

#' Check CouchDB for whether or not plots have been previously created and attached.  If TRUE, then you can skip the work, if FALSE, then you need to make all the plots.
#'
#' It doesn't check each file, just for the 4th plot created, figuring
#' that if that one has been saved, then 1,2,and 3 have also been
#' saved.
#'
#' @param vdsid
#' @param year
#' @param fileprefix
#' @param trackingdb defaults to the usual 'vdsdata%2ftracking
#' @return TRUE or FALSE whether the doc (based on VDSID) has the attached file
#'
#' What this routine does is to re-create the expected filename (I
#' should modularize that routine) and then figure outthe doc id, then
#' will use couchUtils.R to hit the db and check for whether the
#' plot file number 4 has been attached to the database yet.
#'
#' In my code I use this to test whether or not to redo the plots.
check.for.plot.attachment <- function(vdsid,year,
                                      fileprefix=NULL,
                                      trackingdb='vdsdata%2ftracking'){
  imagefileprefix <- paste(vdsid,year,sep='_')
  if(!is.null(fileprefix)){
    imagefileprefix <- paste(vdsid,year,fileprefix,sep='_')
  }
  fourthfile <- paste(imagefileprefix,'004.png',sep='_')
  print(paste('checking for ',fourthfile,'in tracking doc'))
  return (couch.has.attachment(trackingdb,vdsid,fourthfile))
}

#' Plot VDS data and save the resulting plots to the files system and
#' CouchDB tracking database
#'
#' This is more or less a generic function to plot data either before
#' or after running Amelia.  It only works for VDS data of course, but
#' it doesn't care if imputations have been done or not, so indicate
#' so by including a note in the fileprefix parameter
#'
#'
#' @param df.merged the dataframe to plot
#' @param vdsid the VDS id
#' @param year
#' @param fileprefix helps name the output file, and also to find it.
#' By default the plot file will be named via the pattern
#'
#'     imagefileprefix <- paste(vdsid,year,sep='_')
#'
#' But if you include the fileprefix parameter, then the image file
#' naming will have the pattern
#'
#'     imagefileprefix <- paste(vdsid,year,fileprefix,sep='_')
#'
#' So you can add something like "imputed" to the file name to
#' differentiate the imputed plots from the input data plots.
#' @param subhead Written on the plot
#' @param force.plot defaults to FALSE.  If FALSE, and a file exists
#' @param trackingdb defaults to 'vdsdata%2ftracking' for checking if
#' plots already done
#' @return files.to.attach the files that you need to send off to
#' couchdb tracking database.
plot.vds.data  <- function(df.merged,vdsid,year,fileprefix=NULL,subhead='\npost imputation',force.plot=FALSE,trackingdb='vdsdata%2ftracking'){
    if(!force.plot){
        have.plot <- check.for.plot.attachment(vdsid,year,
                                               fileprefix,
                                               trackingdb=trackingdb)
        if(have.plot){
            return (1)
        }
    }
    print('need to make plots')
    varnames <- names(df.merged)
    ## make some diagnostic plots

    ## set up a reconfigured dataframe
    recoded <- recode.df.vds( df.merged )

    ## for coloring occ
    occmidpoint <- mean(sqrt(recoded$occupancy))
    volmidpoint <- mean((recoded$volume))
    daymidpoint <- 12

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

    numlanes <- length(levels(recoded$lane))
    plotheight = 400 * numlanes
    png(file = imagefilename, width=1600, height=plotheight, bg="transparent",pointsize=24)

    p <- ggplot2::ggplot(recoded)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot hourly volume in each lane, by time of day and day of week, for",year,"at site",vdsid,subhead),
                           x="time of day",
                           y="hourly volume per lane")) +
            ggplot2::geom_point(ggplot2::aes(x = tod, y = volume,  color=occupancy),alpha=0.7) +
                ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=occmidpoint,high=("blue"), mid=("red"), low=("yellow"))

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot average hourly occupancy in each lane, by time of day and day of week, for",year,"at site",vdsid,subhead),
                           x="time of day",
                           y="hourly avg occupancy per lane")) +
            ggplot2::geom_point(ggplot2::aes(x = tod, y = occupancy,  color=volume),alpha=0.7) +
                ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=volmidpoint,high=("blue"), mid=("red"), low=("yellow"))

    print(q)


    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot hourly volume vs occupancy in each lane, by day of week, for",year,"at site",vdsid,subhead),
                           y="hourly volume per lane",
                           x="hourly avg occupancy per lane")) +
            ggplot2::geom_point(ggplot2::aes(y = volume, x = occupancy,  color=tod),alpha=0.7) +
                ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=daymidpoint,high=("blue"), mid=("red"), low=("lightblue"),name="hour")

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot hourly volume vs time in each lane, by hour of day, for",year,"at site",vdsid,subhead),
                           y="hourly volume per lane",
                           x="date")) +
            ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=lane),alpha=0.6) +
                ggplot2::facet_wrap(~tod,ncol=4) +
                    ggplot2::scale_color_hue()

    print(q)

    dev.off()

    files.to.attach <- dir(savepath,pattern=paste(imagefileprefix,'0',sep='_'),full.names=TRUE)

    files.to.attach
}

## plot.vds.data.oldway  <- function(df.merged,vdsid,year,fileprefix=NULL,subhead='\npost imputation',force.plot=FALSE){
##     if(!force.plot){
##         have.plot <- check.for.plot.attachment(vdsid,year,fileprefix,subhead)
##         if(have.plot){
##             return (1)
##         }
##     }
##   print('need to make plots')
##   varnames <- names(df.merged)
##   ## make some diagnostic plots
##   ## set up a reconfigured dataframe
##   plotvars <- grep('^n(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)

##   recode <- data.frame()
##   for(nlane in plotvars){
##     lane <- substr(nlane,2,3)
##     olane <- paste('o',lane,sep='')
##     keepvars <- c(nlane,olane,'tod','day','ts')
##     subset <- data.frame(df.merged[,keepvars])
##     subset$lane <- lane
##     names(subset) <- c('n','o','tod','day','ts','lane')
##     if(length(recode)==0){
##       recode <- subset
##     }else{
##       recode <- rbind(recode,subset)
##     }
##   }


##   numlanes <- length(levels(as.factor(recode$lane)))
##   plotheight = 300 * numlanes

##   savepath <- 'images'
##   if(!file.exists(savepath)){dir.create(savepath)}
##   savepath <- paste(savepath,vdsid,sep='/')
##   if(!file.exists(savepath)){dir.create(savepath)}

##   imagefileprefix <- paste(vdsid,year,sep='_')
##   if(!is.null(fileprefix)){
##     imagefileprefix <- paste(vdsid,year,fileprefix,sep='_')
##   }

##   imagefilename <- paste(savepath,paste(imagefileprefix,'%03d.png',sep='_'),sep='/')

##   print(paste('plotting to',imagefilename))

##   png(file = imagefilename, width=900, height=plotheight, bg="transparent",pointsize=24)

##   plotvars <- grep('^n',x=varnames,perl=TRUE,value=TRUE)
##   f <- formula(paste( paste(plotvars,collapse='+' ),' ~ tod | day'))
##   strip.function.a <- strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )
##   strip.function.b <- strip.custom(which.given=2,factor.levels=lane.defs[1:length(plotvars)], strip.levels = TRUE )
##   a <- xyplot( n ~ tod | day + lane, data=recode
##               ,main=paste("Scatterplot volume in each lane, by time of day and day of week, for ",year," at site",vdsid,subhead),
##               ,strip = function(...){
##                 strip.function.a(...)
##                 strip.function.b(...)
##               }
##               ,ylab=list(label='hourly volume, by lane', cex=2)
##               ,xlab=list(label='time of day', cex=2)
##               ,type='p' ## or 'l
##               ,pch='*'
##               ,scales=list(cex=1.8)
##               ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
##               ,panel=pf
##               ,auto.key = list(
##                  space='bottom',
##                  points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
##                  )
##               )
##   print(a)

##   plotvars <- grep('^o(r|l)\\d',x=varnames,perl=TRUE,value=TRUE)
##   f <- formula(paste( paste(plotvars,collapse='+' ),' ~ tod | day'))
##   a <- xyplot( o ~ tod | day + lane, data=recode
##               ,main=paste("Scatterplot occupancy in each lane, by time of day and day of week, for ",year," at site",vdsid,subhead)
##               ,strip = function(...){
##                 strip.function.a(...)
##                 strip.function.b(...)
##               }
##               ,ylab=list(label='hourly occupancy, by lane', cex=2)
##               ,xlab=list(label='time of day', cex=2)
##               ,type='p' ## or 'l'
##               ,pch='*'
##               ,scales=list(cex=1.8)
##               ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
##               ,panel=pf
##               ,auto.key = list(
##                  space='bottom',
##                  points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
##                  )
##               )

##   print(a)


##   a <- xyplot(n ~ o | day + lane, data=recode
##               ,main=paste("Scatterplot hourly volume vs occupancy per lane, by day of week, for ",year," at site",vdsid,subhead)
##               ,strip = function(...){
##                 strip.function.a(...)
##                 strip.function.b(...)
##               }
##               ,ylab=list(label='hourly volume', cex=2)
##               ,xlab=list(label='hourly occupancy', cex=2)
##               ,type='p' ## or 'l'
##               ,pch='*'
##               ,scales=list(cex=1.8)
##               ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
##               ,panel=pf
##               ,auto.key = list(
##                  space='bottom',
##                  points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
##                  )
##               )

##   print(a)

##   strip.function.b <- strip.custom(which.given=1,factor.levels=lane.defs[1:length(plotvars)], strip.levels = TRUE )

##   a <- xyplot(n ~ ts | lane, data=recode
##               ,main=paste("Scatterplot hourly volume vs time, per lane, for ",year," at site",vdsid,subhead)
##               ,strip = function(...){
##                 strip.function.b(...)
##               }
##               ,ylab=list(label='hourly volume', cex=2)
##               ,xlab=list(label='date', cex=2)
##               ,type='p' ## or 'l'
##               ,scales=list(cex=1.8)
##               ,par.settings=simpleTheme(lty=1:length(plotvars),lwd=3)
##               ,auto.key = list(
##                  space='bottom',
##                  points = TRUE, lines = FALSE,columns=length(plotvars),padding.text=10,cex=2
##                  )
##               )
##   print(a)

##   dev.off()

##   files.to.attach <- dir(savepath,pattern=paste(imagefileprefix,'0',sep='_'),full.names=TRUE)
##   for(f2a in files.to.attach){
##     couch.attach(trackingdb,vdsid,f2a)
##   }
##   rm(recode)
##   gc()
##   TRUE
## }

#' Recode VDS dataframe for plotting
#'
#'
#' @param df the dataframe
#' @param plotvars will be extracted fromthe ususal suspects if not
#' present
#' @return will return a new dataframe recoded for easy plotting
recode.df.vds <- function( df ){
    varnames <- names(df)
    plotvars <- grep('^(n|o)(r|l)\\d+',x=varnames,perl=TRUE,value=TRUE)
    n.idx <- grep('^n',x=plotvars,perl=TRUE,value=TRUE)
    o.idx <- grep('^o',x=plotvars,perl=TRUE,value=TRUE)

    melt_1 <- reshape2::melt(data=df,
                             measure.vars=n.idx,
                             id.vars=c('ts','tod','day','obs_count'),
                             variable.name='lane',
                             value.name='volume')
    melt_1$lane <- as.factor(substr(melt_1$lane,2,3))

    melt_2 <- reshape2::melt(data=df,
                             measure.vars=o.idx,
                             id.vars=c('ts','tod','day','obs_count'),
                             variable.name='lane',
                             value.name='occupancy')

    melt_2$lane <- as.factor(substr(melt_2$lane,2,3))

    melded <- merge(x=melt_1,y=melt_2,all=TRUE)

    ## some useful factor things
    melded$day <- factor(melded$day,
                         labels=c('Su','Mo','Tu','We','Th','Fr','Sa'))

    ## lanes?

    lanes <- levels(as.factor(melded$lane))

    lane.names <- c("left","right")
    numbering <- length(lanes)
    for(i in 3:numbering){
        lane.names[i]=paste("lane",(numbering-i+2))
    }

    ## a little switcheroo here
    lanes <- c(lanes[1],rev(lanes[-1]))
    lane.names <- c(lane.names[1],rev(lane.names[-1]))

    melded$lane <- factor(melded$lane,
                          levels=lanes,
                          labels=lane.names)

    melded
}

recode.df.vds.oldway <- function( df,plotvars ){
    recod_df <- NULL
    for(nlane in plotvars){
        lane <- substr(nlane,2,3)
        olane <- paste('o',lane,sep='')
        keepvars <- c(nlane,olane,'tod','day','ts')
        subset <- data.frame(df[,keepvars])
        subset$lane <- lane
        names(subset) <- c('n','o','tod','day','ts','lane')
        recod_df <- rbind(recod_df,subset)
    }
    recod_df
}
