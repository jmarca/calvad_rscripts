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
##' @title good.high.clustering.vds
##' @param df a data frame in the usual idiom for CalVAD amelia stuff,
##'     with variables and timestamp
##' @return a modified dataframe
##' @author James E. Marca
good.high.clustering.vds <- function(df){
    ## copy and paste programming from wim.medianed.aggregate.df, to start off
    varnames <- names(df)
    others <- c('ts','tod','day','obs_count')
    varnames <-  setdiff(varnames,others)
    onames <- grep( pattern="^o",x=varnames,perl=TRUE,invert=FALSE,value=TRUE)
    nnames <- grep( pattern="^n",x=varnames,perl=TRUE,invert=FALSE,value=TRUE)

    df2 <- df
    df2$ts <- as.numeric(df2$ts)

    print(df2$ts[1:10])

    sql_drop_flat <- paste('with',
                           'fake_ts as (',
                           '  select ts as numericts,',
                           "  to_timestamp(ts) AT TIME ZONE 'UTC' as sqlts, * ",
                           'from df2',
                           '),',
                           'dfdoy as (',
                           '  select *,extract (doy from sqlts) as doy,',
                           '  CASE ',
                           "WHEN EXTRACT(DOW FROM sqlts) IN (1,2,3,4,5) THEN 'weekday' ELSE 'weekend' END as weday ",
                           ' from fake_ts ',
                           '),',
                           'dailymax as (',
                           ' select a.*,',
                           paste('max(',
                                 nnames,
                                 ') over (partition by doy order by doy) as max_',
                                 nnames,
                                 sep='',
                                 collapse=', '),
                           ',',
                           paste('min(',
                                 nnames,
                                 ') over (partition by doy order by doy) as min_',
                                 nnames,
                                 sep='',
                                 collapse=', '),
                           'from dfdoy a',
                           '),',
                           ## drop instances in which the daily max equals the daily min
                           ## because that's bad
                           'drop_flat as (',
                           'select ',paste(onames,sep='',collapse=','),',numericts,tod,day,obs_count,doy,weday,',
                           paste(' CASE when max_',nnames,'=min_',nnames,' THEN NULL ELSE ',nnames,' END as ',nnames,sep='',collapse=', '),
                           'from dailymax',
                           '),',
                           ## do the max min thing again
                           'dailymax2 as (',
                           ' select a.*,',
                           paste('max(',
                                 nnames,
                                 ') over (partition by doy order by doy) as max_',
                                 nnames,
                                 sep='',
                                 collapse=', '),
                           ',',
                           paste('min(',
                                 nnames,
                                 ') over (partition by doy order by doy) as min_',
                                 nnames,
                                 sep='',
                                 collapse=', '),
                           'from drop_flat a',
                           '),',
                           'daily_iles as (',
                           'select weday,',
                           paste('percentile_cont(0.05) within group (order by max_',nnames,' ) as twentieth_max_',nnames,sep='',collapse=', '),
                           ',',
                           paste('percentile_cont(0.5) within group (order by max_',nnames,' ) as mid_max_',nnames,sep='',collapse=', '),
                           ',',
                           paste('max( max_',nnames,' ) as max_max_',nnames,sep='',collapse=', '),
                           ',',
                           paste('min( max_',nnames,' ) as min_max_',nnames,sep='',collapse=', '),
                           'from dailymax2 group by weday order by weday',
                           ')',
                           ## now drop all superflat days
                           'select ',
                           paste(' CASE ',
                                 ' WHEN ',
                                 '     (a.max_',nnames,'<= b.twentieth_max_',nnames,') ',
                                 ' THEN NULL ELSE a.',nnames,' END as ',nnames,
                                 sep='',collapse = ', '
                                 ),
                           ## also nullify occupancy for those days
                           ',',
                           paste(' CASE ',
                                 ' WHEN ',
                                 '     (a.max_',nnames,'<= b.twentieth_max_',nnames,') ',
                                 ' THEN NULL ELSE a.',onames,' END as ',onames,
                                 sep='',collapse = ', '
                                 ),
                           ',numericts,tod,day,obs_count',
                           'from ',
                           ' dailymax2 a join ',
                           ' daily_iles b ',
                           ' USING (weday)',
                           ' order by numericts'
                           )

    ## df.minmax <- sqldf::sqldf(sql_two_group,drv="RPostgreSQL")
    df.minmax <- sqldf::sqldf(sql_drop_flat,drv="RPostgreSQL",method="raw")


    ## first two clusters
    ## distance metric is two dimensional I guess, daily min, daily max

    df.minmax$ts <- as.POSIXct(df.minmax$numericts,tz='UTC',origin='1970-01-01')
    attr(df.minmax$ts,'tzone') <- 'UTC'

    print(df.minmax[1:10,])

    df.minmax
}
