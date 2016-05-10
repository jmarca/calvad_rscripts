##' Modify the incoming df to set to NULL all values that fail the max check test
##'
##'
##' The idea here is that the maximum daily hourly count for a
##' variable is unlikely to exceed significantly the median daily
##' hourly count for that variable for the year.  So compute the
##' median maximum daily hourly count, and then compare, say, twice
##' that value to the maximum value for each day.  If the day max
##' exceeds twice the median of the maximums, then throw that day out.
##'
##' This test is done per lane for each variable indicated.  Not sure
##' at the moment how to indicate variables, so I will do it for all
##' variables except time and reduce and/or parameterize from there.
##'
##' @title max.check
##' @param df a data frame in the usual idiom for CalVAD amelia stuff,
##'     with variables and timestamp
##' @return a modified dataframe
##' @author James E. Marca

max.check <- function(df){
    ## copy and paste programming from wim.medianed.aggregate.df, to start off
    varnames <- names(df)
    varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,invert=TRUE,value=TRUE)
    varnames <- setdiff(varnames,c('tod','day'))

    sqltst <-  paste('with dfdoy as (',
                     '  select *,extract (doy from ts) as doy from df',
                     '),',
                     'dailymax as (',
                     ' select a.*,',
                     paste('max(',
                           varnames,
                           ') over (partition by doy order by doy) as max_',
                           varnames,
                           sep='',
                           collapse=', '),
                     ',',
                     paste('min(',
                           varnames,
                           ') over (partition by doy order by doy) as min_',
                           varnames,
                           sep='',
                           collapse=', '),
                     'from dfdoy a',
                     '),',
                     'dailymedian as (',
                     'select day,',
                     paste('percentile_cont(0.5) within group (order by max_',varnames,' ) as median_max_',varnames,sep='',collapse=', '),
                     ',',
                     paste('percentile_cont(0.5) within group (order by min_',varnames,' ) as median_min_',varnames,sep='',collapse=', '),
                     'from dailymax group by day order by day',
                     ')',
                     ## now find the entire days, variables, that exceed the above
                     'select ts,',
                     paste('CASE WHEN a.max_',varnames,' > 1.5*b.median_max_',varnames,
                           ## '      AND a.min_',varnames,' > 1.5*b.median_min_',varnames,
                           ' THEN NULL ELSE a.',varnames,' END as ',varnames,
                           sep='',collapse = ', '
                           ),
                     ',tod,day',
                     'from dailymax a join dailymedian b USING (day)',
                     'order by ts'
                     )

    df.ts <- sqldf::sqldf(sqltst,drv="RPostgreSQL")



    df.ts
}


sqldf_postgresql <- function(config){
    options(sqldf.RPostgreSQL.user = config$postgresql$auth$username,
            sqldf.RPostgreSQL.dbname = 'test', #config$postgresql$db,
            sqldf.RPostgreSQL.host = 'localhost' , #config$postgresql$host,
            sqldf.RPostgreSQL.port = config$postgresql$port)
}

##' Use the median daily max check, and plot both before and after.
##'
##' @title check.wim.with.plots
##' @param df the dataframe to check
##' @param wim.site the wim site number
##' @param direction the direction for this df data
##' @param year the year
##' @param trackingdb the trackingdb (couchdb, for saving plots)
##' @param wim.path the wim path
##' @param config the configuration file with all the passwords, etc
##' @return a new dataframe to use, missing outliers
##' @author James E. Marca
check.wim.with.plots <- function(df,wim.site,direction,year,trackingdb,wim.path,config){

    attach.files <- plot_wim.data(df
                                 ,wim.site
                                 ,direction
                                 ,year
                                 ,fileprefix='raw'
                                 ,subhead='\npre imputation'
                                 ,force.plot=TRUE
                                 ,trackingdb=trackingdb
                                 ,wim.path=wim.path)

    sqldf_postgresql(config)

    df.ts <- max.check(df)

    attach.files <- plot_wim.data(df.ts
                                 ,wim.site
                                 ,direction
                                 ,year
                                 ,fileprefix='raw_limited'
                                 ,subhead='\npre imputation'
                                 ,force.plot=TRUE
                                 ,trackingdb=trackingdb
                                 ,wim.path=wim.path)


    df.ts
}
