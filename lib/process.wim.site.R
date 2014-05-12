##source('./components/jmarca-rstats_couch_utils/couchUtils.R')
source('./utils.R')
source('./wim.impute.functions.R')
source('./wim.aggregate.fixed.R')
source('./wim.loading.functions.R')

source('./amelia_plots_and_diagnostics.R')
source('./get.medianed.amelia.vds.R')
pf <- function(x,y){panel.smoothScatter(x,y,nbin=c(200,200))}

day.of.week <-    c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')
strip.function.a <- strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )


post.impute.plots <- function(wim.site,year,wim.path='/data/backup/wim'){
    ## no need to load raw data
    df.directions <- get.wim.directions(wim.site)
    directions = df.directions$direction
    print(paste(directions,collapse=','))
    for(direction in directions){
        ## reload the imputed wim data
        cdb.wimid <- paste('wim',wim.site,direction,sep='.')
        savepath <- paste(wim.path,year,wim.site,direction,sep='/')
        target.file <- make.amelia.output.file(savepath,paste('wim',wim.site,direction,sep=''),seconds,year)
        print(paste('loading',target.file))
        ## fs write
        load.result <- load(file=target.file)
        print(paste('load result is',load.result))
        if(length(df.wim.amelia) == 1){
            print(paste("amelia run for wim not good",df.wim.amelia))
            next
        }else if(!length(df.wim.amelia)>0 || !length(df.wim.amelia$imputations)>0 || df.wim.amelia$code!=1 ){
            print("amelia run for vds not good")
            next
        }
        ## use zoo to combine a mean value
        df.wim.amelia.c <- df.wim.amelia$imputations[[1]]
        for(i in 2:length(df.wim.amelia$imputations)){
            df.wim.amelia.c <- rbind(df.wim.amelia.c,df.wim.amelia$imputations[[i]])
        }
        print('median of amelia imputations')
        df.zoo <- medianed.aggregate.df(df.wim.amelia.c)

        df.merged <- unzoo.incantation(df.zoo)
        print('make plots')
        make.truck.plots(df.merged,year,wim.site,direction,cdb.wimid,imputed=TRUE)
        rm(df.merged,df.zoo,df.wim.amelia.c,df.wim.amelia)
        gc()
    }
    1
}


process.wim.site <- function(wim.site,year,preplot=TRUE,postplot=TRUE,impute=TRUE,wim.path='/data/backup/wim/'){

    returnval <- 0
    if(!preplot & !postplot & !impute){
        print('nothing to do here, preplot, postplot, and impute all false')
        return()
    }
    directions <- list()
    print(paste('starting to process  wim site ',wim.site))
    df.wim <- load.wim.data.straight(wim.site,year)
    ## only continue if I have real data
    if(dim(df.wim)[1]==0){
        couch.set.state(year=year,detector.id=paste('wim',wim.site,sep='.'),doc=list('imputed'='no wim data in database'))
        return()
    }
    df.wim.split <- split(df.wim, df.wim$direction)
    directions <- names(df.wim.split)
    df.wim.speed <- get.wim.speed.from.sql(wim.site,seconds,year)
    df.wim.speed.split <- split(df.wim.speed, df.wim.speed$direction)
    rm(df.wim)
    rm(df.wim.speed)

    df.wim.dir <- list()
    for(direction in directions){
        ## direction <- names(df.wim.split)[1]
        cdb.wimid <- paste('wim',wim.site,direction,sep='.')
        if(length(df.wim.split[[direction]]$ts)<100){
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='less than 100 timestamps for raw data in db'))
            next
        }
        if(length(df.wim.speed.split[[direction]]$ts)<100){
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='less than 100 timestamps for speed data in db'))
            next
        }

        ## for output files
        if(!file.exists(paste("images",direction,sep='/'))){
            dir.create(paste("images",direction,sep='/'))
        }
        df.wim.d <- process.wim.2(df.wim.split[[direction]])
        df.wim.s <- df.wim.speed.split[[direction]]

        ## fix for site 16, counts of over 100,000 per hour (actually 30 million)
        too.many <- df.wim.s$veh_count > 10000 ## 10,000 veh in 5 minutes!
        df.wim.s <- df.wim.s[!too.many,]


        df.wim.split[[direction]] <- NULL
        df.wim.speed.split[[direction]] <- NULL
        ## gc()

        df.wim.d <- wim.additional.variables(df.wim.d)

        ## aggregate over time
        print(' aggregate ')
        df.wim.dagg <-wim.lane.and.time.aggregation(df.wim.d)
        ## hack around broken speed summaries but instead aborting above.
        ## The one such instance so far had junk measurements
        if(length(df.wim.s)==0){
            ## insert one dummy record per lane
            lastlane <- max(df.wim.d$lane)
            dummytime <- df.wim.d$ts[1]
            df.wim.s <- data.frame(cbind(lane=c('l1',paste('r',2:lastlane,sep=''))))
            df.wim.s$ts <- dummytime
            df.wim.s$veh_speed <- NA
            df.wim.s$veh_count <- NA

        }
        df.wim.sagg <- make.speed.aggregates(df.wim.s)

        ## merge, then explode time using zoo
        df.wim.d.joint <- merge(df.wim.dagg,df.wim.sagg)
        rm(df.wim.dagg, df.wim.sagg,df.wim.s,df.wim.d )
        gc()

        col.names <- names(df.wim.d.joint)
        local.df.wim.agg.ts <- as.ts(df.wim.d.joint)
        rm(df.wim.d.joint)
        ts.ts <- unclass(time(local.df.wim.agg.ts))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
        local.df.wim.agg <- data.frame(ts=ts.ts)
        local.df.wim.agg[col.names] <- local.df.wim.agg.ts ## [,col.names]
        rm(local.df.wim.agg.ts)
        local.df.wim.agg <- add.time.of.day(local.df.wim.agg)
        ##    df.wim.dir[[direction]] <- local.df.wim.agg

        ## gc()
        if(preplot){
            file.pattern <- paste(wim.site,direction,year,'agg.redo',sep='_')

            file.path <- paste(paste('images/',direction,'/',sep=''),file.pattern,sep='')

            png(file = paste(file.path,'%03d.png',sep='_'), width=900, height=600, bg="transparent",pointsize=18)

            plotvars <- grep('not_heavyheavy_',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot non-heavy heavy duty truck counts",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Hour of the day"
                        ,ylab="Non HHD truck counts per hour"
                        ,panel=pf
                        ,auto.key=TRUE)
            print(a)
            plotvars <- grep('^heavyheavy_',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot heavy heavy duty truck counts",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Hour of the day"
                        ,ylab="HHD truck counts per hour"
                        ,panel=pf
                        ,auto.key=TRUE)
            print(a)

            plotvars <- grep('^count_all',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ tod | day'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot counts from summary reports,",year,"  by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Hour of the day"
                        ,ylab="Hourly vehicle counts, all classes"
                        ,panel=pf
                        ,auto.key=TRUE)
            print(a)

            splotvars <- grep('^wgt',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I( (', paste(splotvars,collapse='+' ),') / (', paste(plotvars,collapse='+' ),')) ~ tod | day'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot mean speeds from summary reports,",year," by time of day at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Hour of the day"
                        ,ylab="Hourly mean speeds"
                        ,panel=pf
                        ,auto.key=TRUE)
            print(a)

            ## add plots of data over time

            plotvars <- grep('not_heavyheavy_',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot non-heavy heavy duty truck counts",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Date"
                        ,ylab="non HHD truck counts per hour"
                        ,auto.key=TRUE)
            print(a)

            plotvars <- grep('^heavyheavy_',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot heavy heavy duty truck counts",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Date"
                        ,ylab="HHD truck counts per hour"
                        ,auto.key=TRUE)
            print(a)

            plotvars <- grep('^count_all',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I(', paste(plotvars,collapse='+' ),') ~ ts'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot counts from summary reports,",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Date"
                        ,ylab="Hourly vehicle counts, all classes"
                        ,auto.key=TRUE)
            print(a)

            splotvars <- grep('^wgt',x=names(local.df.wim.agg),perl=TRUE,value=TRUE)
            f <- formula(paste('I( (', paste(splotvars,collapse='+' ),') / (', paste(plotvars,collapse='+' ),')) ~ ts'))
            a <- xyplot(f
                        ,data=local.df.wim.agg
                        ,main=paste("Scatterplot mean speeds from summary reports,",year," over time at site",wim.site,direction,"\nRevised method, pre-imputation")
                        ,strip=strip.function.a
                        ,xlab="Date"
                        ,ylab="Hourly mean speeds"
                        ,auto.key=TRUE)
            print(a)

            dev.off()
            if(!impute){
                rm (local.df.wim.agg)
                gc()
            }

            files.to.attach <- dir(file.path,pattern=paste("^",file.pattern,sep=''),full.names=TRUE)

            for(f2a in files.to.attach){
                couch.attach(trackingdb
                             ,cdb.wimid
                             ,f2a
                             ,local=TRUE
                             )
            }

        }
        if(impute){

            ## direction <- names(df.wim.split)[1]
            print(paste(year,wim.site,direction))
            couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='started'))

            ## save and move on to the next one

            ## but save where?
            ## to couchdb as hourly data??
            ## no, to fs on lysithia.  brief digression to write that into my node server.

            savepath <- paste(wim.path,year,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            savepath <- paste(savepath,wim.site,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            savepath <- paste(savepath,direction,sep='/')
            if(!file.exists(savepath)){dir.create(savepath)}
            filepath <- paste(savepath,'wim.agg.RData',sep='/')

            db.legal.names  <- gsub("\\.", "_", names(local.df.wim.agg))

            names(local.df.wim.agg) <- db.legal.names
            save(local.df.wim.agg,file=filepath,compress='xz')
            print('amelia run')

            r <- try(
                df.wim.amelia <- fill.wim.gaps(local.df.wim.agg
                                               ,count.pattern='^(not_heavyheavy|heavyheavy|count_all_veh_speed)'
                                               )
                )
            if(class(r) == "try-error") {
                returnval <- paste(r,'')
                print(paste('try error:',r))
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'=paste('try error',r)))
            }

            if(df.wim.amelia$code==1){
                ## that means good imputation
                ## have a WIM site data with no gaps.  save it
                target.file <- make.amelia.output.file(savepath,paste('wim',wim.site,direction,sep=''),seconds,year)
                print(paste('name is',target.file,'savepath is',savepath))
                ## fs write
                save(df.wim.amelia,file=target.file,compress="xz")
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'='finished'))
                returnval <- 1
            }else{
                returnval <- paste(df.vds.agg.imputed$code,'message',df.vds.agg.imputed$message)
                print(paste("amelia not happy:",returnval))
                couch.set.state(year=year,detector.id=cdb.wimid,doc=list('imputed'=paste('error:',returnval)))
            }
            rm(df.wim.amelia,local.df.wim.agg)
            gc()
        }

    }

    returnval
}
