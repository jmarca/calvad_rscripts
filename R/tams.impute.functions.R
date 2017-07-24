
##' Run Amelia to fill in missing values in the TAMS observations
##'
##'
##' This function will ##' run Amelia to fill in missing values in the
##' TAMS observations
##' @title fill.tams.gaps
##' @param df.tams the TAMS dataframe, typically not the raw obs but
##'     rather aggregated up to one hour of observation data
##' @param count.pattern the regex to use to identify count variables
##' @param plotfile the name of a plotfile, I guess.  If something
##'     other than an empty string (the default) then the standard
##'     Amelia plot will be run and saved to this file (using that
##'     filename as a base pattern).  If it is empty, then no plots
##'     will be run
##' @return an Amelia object, or some string complaining that there's
##'     nothing for it but to give up
##' @author James E. Marca
fill.tams.gaps <- function(df.tams
                         ,count.pattern='^(not_heavyheavy_|heavyheavy_|n_)'
                         ,plotfile=""){


    ic.names <- names(df.tams)
    count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
    M <- 10000000000  #arbitrary bignum for max limits

    ## now build up all the final variables for the amelia call



    ## generate the position of each count variable
    pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]

    ## for the count variables,
    ## make a max allowed value of 110% of observed values and a min value of 0
    max.vals <- apply( df.tams[,count.vars], 2, max ,na.rm=TRUE)
    min.vals <- apply( df.tams[,count.vars], 2, min ,na.rm=TRUE)

    pos.bds <- cbind(pos.count,min.vals,1.10*max.vals)

    print("bounds:")
    print(pos.bds)


    ## so instead of listing each, figure out which aren't

    exclude.as.id.vars <- setdiff(ic.names,c(count.vars,'tod','day'))

  print(paste("excluded:",  paste(exclude.as.id.vars,collapse=' ')))

  df.tams.amelia <-
      Amelia::amelia(df.tams,
                     idvars=exclude.as.id.vars,
                     ts="tod",
                     splinetime=6,
                     lags =count.vars,
                     leads=count.vars,
                     cs="day",intercs=TRUE,
                     emburn=c(2,100),
                     bounds = pos.bds,
                     max.resample=10
                     ## ,empri = 0.05 *nrow(df.tams)
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
      plot(df.tams.amelia,compare=TRUE,overimpute=TRUE,ask=FALSE)
      dev.off()
  }

  df.tams.amelia
}

##' tams medianed aggregate df
##'
##' Generate a hourly aggregate of the impuation results
##'
##' @title tams.medianed.aggregate.df
##' @param df the mulitple imputations, optionally rbind'ed into a
##'     single dataframe.  Probably best to just pass in the output
##'     from Amelia here and let this routine do the rbinding.  But if
##'     perhaps there are multiple Amelia runs, then you can do the
##'     rbinding outside of this function
##' @param op defaults to median, but you can also try mean.  This is
##'     how the multiple imputations are merged into a single
##'     imputation result
##'
##' @return a dataframe holding one entry per hour, with the multiple
##' imputations aggregated according to \code{op} according to whatever
##' time step the amelia run was done, and then aggregated up to one
##' hour by summing the counts and averaging the occupancies
##'
##' @author James E. Marca
##' @export
tams.medianed.aggregate.df <- function(df,op=median){
    if(class(df) == 'amelia'){
        ## fix that
        aout <- df
        df <- NULL
        for(i in 1:length(aout$imputations)){
            df <- rbind(df,aout$imputations[[i]])
        }
    }
    varnames <- names(df)
    varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,invert=TRUE,value=TRUE)
    varnames <- setdiff(varnames,c('tod','day','hr'))

    ## use sqldf...so much faster than zoo, aggregate

    sqlstatement <- paste("select ts,",
                          paste('median(',varnames,') as ',varnames,sep=' ',collapse=','),
                          'from df group by ts',
                          sep=' ',collapse=' '
                          )

    ## aggregate the multiple imputations, resulting in one value per
    ## time step
    ## print(sqlstatement)
    temp_df <- sqldf::sqldf(sqlstatement,drv="SQLite")
    attr(temp_df$ts,'tzone') <- 'UTC'

    ## at the moment this is a no-op, because I am imputing TAMS data
    ## at one hour.  But keep it because it might be useful in the
    ## future
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
    ## print(sqlstatement2)
    ## generate the hourly summation
    df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")

    ## assign the correct timezone again
    attr(df_hourly$ts,'tzone') <- 'UTC'

    df_hourly$tick <- NULL

    df_hourly <- add.time.of.day(df_hourly)

    ## all done, return value
    df_hourly
}

##' Load TAMS imputed (Amelia output) data from a saved RData file.
##'
##' This function will use the provided site number, year, and
##' direction to look below the tams.path to find if a suitable RData
##' file exists.  If so, it will load it and return it as a
##' data.frame.  If not, it will return the character string 'todo'.
##' @title get.amelia.tams.file.local
##' @param tams.site the tams site
##' @param year the year
##' @param direction the direction, 'N', 'S', 'E', or 'W'
##' @param tams.path the relative path to where to find TAMS files
##' @return a data frame or a string
##' @author James E. Marca
##' @export
get.amelia.tams.file.local <- function(tams.site,year,direction,tams.path){

    savepath <- paste(tams.path,year,tams.site,direction,sep='/')
    target.file <- make.amelia.output.pattern(
        fname=paste('tams',tams.site,direction,sep='')
       ,year=year)
    target.file <- paste(target.file,'$',sep='')

    isa.df <- dir(savepath, pattern=target.file,full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
  ## print(paste(path,target.file,paste(isa.df,collapse=','),sep=' : '))
    if(length(isa.df)==0){
        return('todo')
    }
    ## keep the file with the correct year
    right_file <- grep(pattern=year,x=isa.df,value=TRUE)
    if(length(right_file) == 0){
        print(paste('failed to find year',year,'in list',paste(isa.df,collapse=',')))
        return('todo')

    }
    if(length(right_file) > 1){
        print(paste('failed to isolate one file from list',paste(isa.df,collapse=','),'got',paste(right_file,collapse=',')))
        stop(2)

    }
    env <- new.env()
    res <- load(file=right_file,envir=env)
    return (env[[res]])

}
