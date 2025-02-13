library(reshape2)

df.melted <- melt(df.vds.agg.imputed$imputations[i])
ggplot(data = df.melted, aes(x = x, y = value)) +
    geom_point() + facet_grid(variable ~ .)


b.out <- NULL
se.out <- NULL
m <-  5
for(i in 1:m) {
    ols.out <- lm(civlib ~ trade ,data = df.vds.agg.imputed$imputations[[i]])
    b.out <- rbind(b.out, ols.out$coef)
    se.out <- rbind(se.out, coef(summary(ols.out))[,2])
}
combined.results <- mi.meld(q = b.out, se = se.out)

df.combined <-  NULL
for(i in 1:df.vds.agg.imputed$m){
    df.combined <- rbind(df.combined,df.vds.agg.imputed$imputations[[i]])
}

varnames <- names(df.combined)
varnames <- grep( pattern="^ts",x=varnames,perl=TRUE,inv=TRUE,value=TRUE)

n.names <- grep(pattern="^n(l|r)\\d+",x=varnames,perl=TRUE,value=TRUE)
o.names <- grep(pattern="^o(l|r)\\d+",x=varnames,perl=TRUE,value=TRUE)
other.names <- grep(pattern="^(n|o)(l|r)\\d+",x=varnames,inv=TRUE,perl=TRUE,value=TRUE)

## use sqldf!

sqlstatement <- paste("select ts,",
                      paste('median(',c(n.names,o.names),') as ',c(n.names,o.names),sep=' ',collapse=','),
                      ',',paste(other.names,sep='',collapse=','),
                      'from df_combined group by ts',
                      sep=' ',collapse=' '
                            )

temp_df <- sqldf(sqlstatement)
attr(temp_df$ts,'tzone') <- 'UTC'

temp_df$tick <- 1
hour <-  3600
temp_df$hourly <- as.numeric(temp_df$ts) - as.numeric(temp_df$ts) %% hour

sqlstatement2 <- paste("select min(ts) as ts,",
                      paste('sum(',c(n.names,o.names,'obs_count','tick'),') as ',c(n.names,o.names,'obs_count','tick'),sep=' ',collapse=','),
                      ',tod,day',
                      'from temp_df group by hourly',
                      sep=' ',collapse=' '
                            )
df_hourly <- sqldf(sqlstatement2)

attr(df_hourly$ts,'tzone') <- 'UTC'

df_hourly[,o.names] <- df_hourly[,o.names]/df_hourly$tick


## quick check of using "equals" for data frame testing
obj1 <- data.frame(nn=n.names,oo=o.names)
obj2 <- data.frame(nn=n.names,oo=o.names)

test_that(obj1,equals(obj2))


seconds <- 120

irritating.ts <- seq(df.agg$ts[1],df.agg$ts[len],by=seconds)

timeslot <- as.numeric(ts) - as.numeric(ts) %% seconds


## keep table old way
keep
 FALSE   TRUE
  2310 205792
[1] 263520

## keep table new way
 FALSE   TRUE
  2310 205792
keep
 FALSE   TRUE
 57728 205792

   0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
 784 1869 3497 4749 5202 4958 4413 3766 3360 2870 2557 2348 2049 2037 1953 1883
  16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
1779 1652 1664 1592 1587 1538 1570 1562 1508 1473 1484 1598 1791 1738 1777 1929
  32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
2089 2295 2348 2465 2459 2615 2566 2595 2521 2585 2520 2373 2421 2494 2410 2440
  48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
2436 2388 2405 2548 2608 2668 2716 2710 2913 3068 2964 3182 3317 3445 3543 3493
  64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
3330 3382 3409 3380 3232 3067 2948 2766 2622 2457 2167 1957 1792 1569 1435 1187
  80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
 957  858  718  567  479  388  272  209  158  112   72   49   34   35   16    5
  96   97   98   99  100  101
   7    6    3    3    3    1

oldway
   0    1    2    3    4    5    6    7    8    9   10   11   12   13   14   15
 787 1869 3497 4749 5202 4958 4413 3766 3360 2870 2557 2348 2049 2037 1953 1883
  16   17   18   19   20   21   22   23   24   25   26   27   28   29   30   31
1779 1652 1664 1592 1587 1538 1570 1562 1508 1473 1484 1598 1791 1738 1777 1929
  32   33   34   35   36   37   38   39   40   41   42   43   44   45   46   47
2089 2295 2348 2465 2459 2615 2566 2595 2521 2585 2520 2373 2421 2494 2410 2440
  48   49   50   51   52   53   54   55   56   57   58   59   60   61   62   63
2436 2388 2405 2548 2608 2668 2716 2710 2913 3068 2964 3182 3317 3445 3543 3493
  64   65   66   67   68   69   70   71   72   73   74   75   76   77   78   79
3330 3382 3409 3380 3232 3067 2948 2766 2622 2457 2167 1957 1792 1569 1435 1187
  80   81   82   83   84   85   86   87   88   89   90   91   92   93   94   95
 957  858  718  567  479  388  272  209  158  112   72   49   34   35   16    5
  96   97   98   99  100  101
   7    6    3    3    3    1
>


,"obs_count"

sp <- ggplot(twerked.df, aes(x=ts, y=volume)) + geom_point(shape=1)

sp + facet_grid(day ~ .)

sp + facet_wrap( ~ tod, ncol=4)


twerked.df$day <- factor(twerked.df$day,
                         labels=c('Su','Mo','Tu','We','Th','Fr','Sa'))

twerked.df$lane <- factor(twerked.df$lane,
                          levels=c('l1','r3','r2','r1'),
                          labels=c('left','lane2','lane3','right'))

sp <- ggplot(twerked.df, aes(x=ts, y=volume, color=day))

sp + geom_point(shape=1) + facet_wrap( ~ tod, ncol=4)

sp + geom_point(shap=1) + facet_wrap(~lane,ncol=2)+scale_colour_hue("day")

(sp <- qplot(ts, volume, data=twerked.df, colour=day))

sp+scale_color_hue()


sp <- qplot(ts, volume, data=twerked.df, colour=occupancy)

sp+scale_color_gradientn(colours = rainbow(5))+geom_point(alpha=occupancy)+ facet_wrap(~lane,ncol=2)

p <- ggplot2::ggplot(twerked.df)

p + ggplot2::geom_point(ggplot2::aes(x = ts, y = volume, alpha=occupancy, color=day))+ ggplot2::facet_wrap(~lane,ncol=2)

p + ggplot2::geom_point(ggplot2::aes(x = ts, y = volume, color=day),alpha=0.6)+ ggplot2::facet_wrap(~lane,ncol=2)

p + ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=occupancy),alpha=0.6)+ ggplot2::facet_wrap(~lane,ncol=2)+ggplot2::scale_color_gradient()

library(scales) # for muted

## per lane
p + ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=occupancy),alpha=0.3)+ ggplot2::facet_wrap(~lane,ncol=2)+ggplot2::scale_color_gradient2(midpoint=0.23,high=("blue"), low=muted("red"))

p +
    ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=occupancy),alpha=0.7) +
        ggplot2::facet_wrap(~lane,ncol=2)+
            ggplot2::scale_color_gradient2(midpoint=0.23,high=("blue"), mid=("red"), low=("yellow"))

## per hour
p +
    ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=occupancy),alpha=0.6) +
        ggplot2::facet_wrap(~tod,ncol=4) +
            ggplot2::scale_color_gradient2(midpoint=0.23,high=("blue"), mid=("red"), low=("yellow"))

## p + ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=occupancy),alpha=0.50)+ ggplot2::facet_wrap(~tod,ncol=4)+ggplot2::scale_color_gradient2(midpoint=0.23,high=("blue"), low=muted("red"))




## hack to see about lanes labeling
## number of lanes, then drop one for decrement

vdsid <- 123456
subhead <- 'after imputation of mising values'

occmidpoint <- mean(sqrt(twerked.df$occupancy))

volmidpoint <- mean((twerked.df$volume))

daymidpoint <- 12

### plot 1
p +
    ggplot2::labs(list(title=paste("Scatterplot hourly volume in each lane, by time of day and day of week, for",year,"at site",vdsid,subhead),
                       x="time of day",
                       y="hourly volume per lane")) +
        ggplot2::geom_point(ggplot2::aes(x = tod, y = volume,  color=occupancy),alpha=0.7) +
            ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=occmidpoint,high=("blue"), mid=("red"), low=("yellow"))

### plot 2

p +
    ggplot2::labs(list(title=paste("Scatterplot average hourly occupancy in each lane, by time of day and day of week, for",year,"at site",vdsid,subhead),
                       x="time of day",
                       y="hourly avg occupancy per lane")) +
        ggplot2::geom_point(ggplot2::aes(x = tod, y = occupancy,  color=volume),alpha=0.7) +
            ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=volmidpoint,high=("blue"), mid=("red"), low=("yellow"))

### plot 3

p +
    ggplot2::labs(list(title=paste("Scatterplot hourly volume vs occupancy in each lane, by day of week, for",year,"at site",vdsid,subhead),
                       y="hourly volume per lane",
                       x="hourly avg occupancy per lane")) +
        ggplot2::geom_point(ggplot2::aes(y = volume, x = occupancy,  color=tod),alpha=0.7) +
            ggplot2::facet_grid(lane~day)+
                    ggplot2::scale_color_gradient2(midpoint=daymidpoint,high=("blue"), mid=("red"), low=("lightblue"),name="hour")

### plot 4

## per hour
p +
    ggplot2::labs(list(title=paste("Scatterplot hourly volume vs time in each lane, by hour of day, for",year,"at site",vdsid,subhead),
                       y="hourly volume per lane",
                       x="date")) +
    ggplot2::geom_point(ggplot2::aes(x = ts, y = volume,  color=lane),alpha=0.6) +
        ggplot2::facet_wrap(~tod,ncol=4) +
            ggplot2::scale_color_hue()
