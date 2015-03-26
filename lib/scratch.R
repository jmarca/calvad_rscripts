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
