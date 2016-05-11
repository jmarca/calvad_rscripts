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


##' Modify the incoming df to set to NULL all values that fail the max check test
##'
##'
##' The idea with this one is to use clusters of good and too high
##' data, versus a single cluster of all good data.  If the all good
##' assumption is valid, then that would mean that the spread of the
##' data across days is fine.  If it isn't a good assumption, then the
##' spread in the lower, good values set is good as is the spread in
##' the bad values, too high set.
##'
##' I could also do three clusters I guess, one too low.
##'
##' This test is done per lane for each variable indicated.  Not sure
##' at the moment how to indicate variables, so I will do it for all
##' variables except time and reduce and/or parameterize from there.
##'
##' @title good.high.clustering
##' @param df a data frame in the usual idiom for CalVAD amelia stuff,
##'     with variables and timestamp
##' @return a modified dataframe
##' @author James E. Marca
good.high.clustering <- function(df){
    ## copy and paste programming from wim.medianed.aggregate.df, to start off
    varnames <- names(df)
    varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,invert=TRUE,value=TRUE)
    varnames <- setdiff(varnames,c('tod','day'))

    sql_two_group <- paste('with dfdoy as (',
                           '  select *,extract (doy from ts) as doy,',
                           "  CASE WHEN EXTRACT(DOW FROM ts) IN (1,2,3,4,5) THEN 'weekday' ELSE 'weekend' END as weday ",
                           ' from df ',
                     '),',
                     'dailymax as (',
                     ' select a.*,',
                     paste('max(',
                           varnames,
                           ') over (partition by doy order by doy) as max_',
                           varnames,
                           sep='',
                           collapse=', '),
                     ## ',',
                     ## paste('min(',
                     ##       varnames,
                     ##       ') over (partition by doy order by doy) as min_',
                     ##       varnames,
                     ##       sep='',
                     ##       collapse=', '),
                     'from dfdoy a',
                     '),',
                     'daily_iles as (',
                     'select weday,',
                     paste('percentile_cont(0.25) within group (order by max_',varnames,' ) as quart_max_',varnames,sep='',collapse=', '),
                     ',',
                     paste('percentile_cont(0.5) within group (order by max_',varnames,' ) as mid_max_',varnames,sep='',collapse=', '),
                     ',',
                     paste('percentile_cont(0.75) within group (order by max_',varnames,' ) as sept_max_',varnames,sep='',collapse=', '),
                     ',',
                     paste('max( max_',varnames,' ) as max_max_',varnames,sep='',collapse=', '),
                     ',',
                     paste('min( max_',varnames,' ) as min_max_',varnames,sep='',collapse=', '),
                     'from dailymax group by weday order by weday',
                     '),',
                     ## pick whether to cluster on quart,mid,sept, or max
                     'pick_two as (',
                     '  select weday, ',
                     paste(' a.max_max_',varnames,'<1.9*a.quart_max_',varnames,' as oneclust_',varnames,sep='',collapse=', '),', ',
                     paste(' CASE ',
                           ## don't care if the spread isn't all that big
                           ' WHEN a.max_max_',varnames,'<2*a.min_max_',varnames,' THEN a.mid_max_',varnames,
                           ## if mid is closer to max than min, then use 25 or min
                           ' WHEN @ (a.max_max_',varnames,'-a.mid_max_',varnames,') ',
                           '    < @ (a.min_max_',varnames,'-a.mid_max_',varnames,') ',
                           ## AND
                           '   AND @ (a.max_max_',varnames,'-a.quart_max_',varnames,') ',
                           '     < @ (a.min_max_',varnames,'-a.quart_max_',varnames,') ',
                           ' THEN a.min_max_',varnames,
                           ## if still here and mid is closer to max than min, then use 25
                           ' WHEN @ (a.max_max_',varnames,'-a.mid_max_',varnames,') ',
                           '    < @ (a.min_max_',varnames,'-a.mid_max_',varnames,') ',
                           ## AND because of the prior case, quart is closer to min than max
                           ' THEN a.quart_max_',varnames,
                           ## if *still* here, then mid is closer to min than max, so use mid
                           ' ELSE a.mid_max_',varnames,
                           ' END as cl_',varnames,sep='',collapse=', '),
                     'FROM daily_iles a',
                     ')',
                     ## now guess whether to group by higher or lower
                     'select ts, ',
                     paste(' CASE ',
                           ' WHEN b.oneclust_',varnames,' THEN a.',varnames,
                           ' WHEN ',
                           '     (a.max_',varnames,'>1.8*b.cl_',varnames,') ',
                           ' THEN NULL ELSE a.',varnames,' END as ',varnames,
                           sep='',collapse = ', '
                           ),
                     ',tod,day',
                     'from ',
                     ' dailymax a join ',
                     ' pick_two b ',
                     ' USING (weday)',
                      ' order by ts'
                     )

    df.minmax <- sqldf::sqldf(sql_two_group,drv="RPostgreSQL")

    ## first two clusters
    ## distance metric is two dimensional I guess, daily min, daily max


        ## attach.files2 <- plot_wim.data(df.minmax
        ##                          ,wim.site
        ##                          ,direction
        ##                          ,year
        ##                          ,fileprefix='raw_twogrouped_b'
        ##                          ,subhead='\npre imputation'
        ##                          ,force.plot=TRUE
        ##                          ,trackingdb=trackingdb
        ##                          ,wim.path=wim.path)

    df.minmax
}


sqldf_postgresql <- function(config){
    options(sqldf.RPostgreSQL.user = config$postgresql$auth$username,
            sqldf.RPostgreSQL.dbname = config$postgresql$db,
            sqldf.RPostgreSQL.host = config$postgresql$host,
            sqldf.RPostgreSQL.port = config$postgresql$port)
}
