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


    df2 <- df
    df2$ts <- as.numeric(df2$ts)

    randomnumber <- floor(runif(1, 100000,900000))

    variablename <- paste('df',randomnumber,sep='_')
    assign(variablename,df2)

    sql_two_group <- paste('with',
                           'fake_ts as (',
                           '  select ts as numericts,',
                           "  to_timestamp(ts) AT TIME ZONE 'UTC' as sqlts, * ",
                           'from ',variablename,
                           '),',
                           'dfdoy as (',
                           '  select *,extract (doy from sqlts) as doy,',
                           "  CASE WHEN EXTRACT(DOW FROM sqlts) IN (1,2,3,4,5) THEN 'weekday' ELSE 'weekend' END as weday ",
                           ' from fake_ts ',
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
                     ## drop instances in which the daily max equals the daily min
                     ## because that's bad
                     'drop_flat as (',
                     'select numericts,tod,day,doy,weday,',
                     paste(' CASE when max_',varnames,'=min_',varnames,' THEN NULL ELSE ',varnames,' END as ',varnames,sep='',collapse=', '),
                     'from dailymax',
                     '),',
                     ## do the max min thing again
                     'dailymax2 as (',
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
                     'from drop_flat a',
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
                     'from dailymax2 group by weday order by weday',
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
                     'select numericts, ',
                     paste(' CASE ',
                           ' WHEN b.oneclust_',varnames,' THEN a.',varnames,
                           ' WHEN ',
                           '     (a.max_',varnames,'>1.8*b.cl_',varnames,') ',
                           ' THEN NULL ELSE a.',varnames,' END as ',varnames,
                           sep='',collapse = ', '
                           ),
                     ',tod,day',
                     'from ',
                     ' dailymax2 a join ',
                     ' pick_two b ',
                     ' USING (weday)',
                      ' order by numericts'
                     )

    df.minmax <- sqldf::sqldf(sql_two_group,drv="RPostgreSQL")
    df.minmax$ts <- as.POSIXct(df.minmax$numericts,tz='UTC',origin='1970-01-01')
    attr(df.minmax$ts,'tzone') <- 'UTC'

    df.minmax$numericts <- NULL

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
            sqldf.RPostgreSQL.password = '',
            sqldf.RPostgreSQL.dbname = config$postgresql$db,
            sqldf.RPostgreSQL.host = config$postgresql$host,
            sqldf.RPostgreSQL.port = config$postgresql$port)
}
