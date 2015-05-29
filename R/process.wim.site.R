pf <- function(x,y){lattice::panel.smoothScatter(x,y,nbin=c(200,200))}

## day.of.week <-    c('Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday')
## lane.defs <- c('left lane','right lane 1', 'right lane 2', 'right lane 3', 'right lane 4', 'right lane 5', 'right lane 6', 'right lane 7', 'right lane 8')
strip.function.a <- lattice::strip.custom(which.given=1,factor.levels=day.of.week, strip.levels = TRUE )

#' get Amelia WIM file from the local file system
#'
#' Rather than hitting a remote server, just get the Amelia output
#' from the local file system.  Use this if everything is running on
#' one machine, ya?
#'
#' @param site_no the WIM site number
#' @param direction the direction of the data for this WIM site
#' @param path the root path of where the Amelia files are stashed
#' @param year the year of the data
#' @return either 'todo' indicating that the file is not on this
#' machine or not yet done, or a dataframe containing the amelia
#' output
#' @export
get.amelia.wim.file.local <- function(site_no
                                     ,direction
                                     ,path='/'
                                     ,year){

    wimid <- paste('wim',site_no,direction,sep='')

    file_pattern <- make.amelia.output.pattern(wimid,year)
    isa.df <- dir(path
                 ,pattern=file_pattern
                 ,full.names=TRUE
                 ,ignore.case=TRUE
                 ,recursive=TRUE)
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


## must modularize this more

#' Process a WIM site's data, including pre-plots, imputation of
#' missing values, and post-plots.
#'
#' This is the main routine.
#'
#' @param wim.site the wim site
#' @param year the year
#' @param seconds the number of seconds to aggregate.  Almost always
#' will be 3600 (which is one hour)
#' @param preplot TRUE or FALSE, defaults to TRUE
#' @param postplot TRUE or FALSE, defaults to TRUE
#' @param impute TRUE or FALSE, defaults to TRUE
#' @param force.plot TRUE or FALSE.  whether to redo the plots even if
#' they are already in couchdb
#' @param wim.path where the WIM data can be found on the local file
#' @param trackingdb the usual "vdsdata\%2ftracking"
#' system.  Default is '/data/backup/wim' because that is the
#' directory on the machine I developed this function on
#' @param con a postgresql db connection for loading WIM data
#' @return either 0, 1, or the result of running the imputation if it
#' failed.  Also check the trackingdb for any mention of issues.
#' @export
process.wim.site <- function(wim.site,
                             year,
                             seconds=3600,
                             preplot=TRUE,
                             postplot=TRUE,
                             impute=TRUE,
                             force.plot=FALSE,
                             wim.path='/data/backup/wim/',
                             trackingdb='vdsdata%2ftracking',
                             con){

    print(paste('wim path is ',wim.path))

    returnval <- list()
    if(!preplot & !postplot & !impute){
        print('nothing to do here, preplot, postplot, and impute all false')
        return(0)
    }

    print(paste('starting to process  wim site ',wim.site))

    ## two cases.  One, I'm redoing work and can just skip to the
    ## impute step.  Two, I need to hit the db directly.  Figure it
    ## out by checking if I can load the data from the file

    df.wim <- load.wim.data.straight(wim.site=wim.site,year=year,con=con)
    ## only continue if I have real data
    if(dim(df.wim)[1]==0){
        print(paste('problem, dim df.wim is',dim(df.wim)))
        rcouchutils::couch.set.state(year=year,
                        id=paste('wim',wim.site,sep='.'),
                        doc=list('imputed'='no wim data in database'),
                        db=trackingdb)
        return(0)
    }

    df.wim.split <- split(df.wim, df.wim$direction)
    directions <- names(df.wim.split)
    df.wim.speed <- get.wim.speed.from.sql(wim.site=wim.site,year=year,con=con)
    df.wim.speed.split <- split(df.wim.speed, df.wim.speed$direction)
    rm(df.wim)
    rm(df.wim.speed)

    for(direction in directions){
        print(paste('processing direction',direction))
        ## direction <- names(df.wim.split)[1]
        cdb.wimid <- paste('wim',wim.site,direction,sep='.')
        if(length(df.wim.split[[direction]]$ts)<100){
            rcouchutils::couch.set.state(year=year,
                            id=cdb.wimid,
                            doc=list('imputed'='less than 100 timestamps for raw data in db'),
                            db=trackingdb)
            next
        }
        if(length(df.wim.speed.split[[direction]]$ts)<100){
            rcouchutils::couch.set.state(year=year,
                            id=cdb.wimid,
                            doc=list('imputed'='less than 100 timestamps for speed data in db'),
                            db=trackingdb)
            next
        }

        df.wim.d <- process.wim.2(df.wim.split[[direction]])
        df.wim.s <- df.wim.speed.split[[direction]]

        ## fix for site 16, counts of over 100,000 per hour (actually 30 million)
        too.many <- df.wim.s$veh_count > 10000 ## 10,000 veh in 5 minutes!
        df.wim.s <- df.wim.s[!too.many,]


        df.wim.split[[direction]] <- NULL
        df.wim.speed.split[[direction]] <- NULL

        df.wim.d <- wim.additional.variables(df.wim.d)

        ## aggregate over time
        print(' aggregate ')
        df.wim.dagg <- wim.lane.and.time.aggregation(df.wim.d)

        ## ... instead aborting above.
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

        df.wim.d.joint <- merge(df.wim.dagg,df.wim.sagg,all=TRUE)

        rm(df.wim.dagg, df.wim.sagg,df.wim.s,df.wim.d )

        df.wim.d.joint <- add.time.of.day(df.wim.d.joint)

        if(preplot){
            attach.files <- plot_wim.data(df.wim.d.joint
                                         ,wim.site
                                         ,direction
                                         ,year
                                         ,fileprefix='raw'
                                         ,subhead='\npre imputation'
                                         ,force.plot=force.plot
                                         ,trackingdb=trackingdb)
            if(attach.files != 1){
                for(f2a in c(attach.files)){
                    rcouchutils::couch.attach(trackingdb,cdb.wimid,f2a)
                }
            }
        }

        ## save wim data for next time

        savepath <- paste(wim.path,year,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,wim.site,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,direction,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        filepath <- paste(savepath,'wim.agg.RData',sep='/')
        print(filepath)

        db.legal.names  <- gsub("\\.", "_", names(df.wim.d.joint))

        names(df.wim.d.joint) <- db.legal.names
        save(df.wim.d.joint,file=filepath,compress='xz')
        print(paste('saved to',filepath))
        if(impute){

            print(paste('imputing',year,wim.site,direction))
            r <- try(
                df.wim.amelia <- fill.wim.gaps(df.wim.d.joint)
                )
            if(class(r) == "try-error") {
                returnval[[direction]] <- paste(r,'')
                print(paste('try error:',r))
                rcouchutils::couch.set.state(year=year,
                                id=cdb.wimid,
                                doc=list('imputed'=paste('try error',r)),
                                db=trackingdb)
            }

            returnval[[direction]] <- df.wim.amelia
            if(df.wim.amelia$code==1){
                ## that means good imputation
                ## have a WIM site data with no gaps.  save it
                target.file <- make.amelia.output.file(savepath,paste('wim',wim.site,direction,sep=''),seconds,year)
                print(paste('name is',target.file,'savepath is',savepath))
                ## fs write
                save(df.wim.amelia,file=target.file,compress="xz")
                rcouchutils::couch.set.state(year=year,
                                id=cdb.wimid,
                                doc=list('imputed'='finished'),
                                db=trackingdb)

                if(postplot){
                    df.wim.agg.amelia <- wim.medianed.aggregate.df(df.wim.amelia)
                    attach.files <- plot_wim.data(df.wim.agg.amelia
                                                 ,wim.site
                                                 ,direction
                                                 ,year
                                                 ,fileprefix='imputed'
                                                 ,subhead='\npost imputation'
                                                 ,force.plot=force.plot
                                                 ,trackingdb=trackingdb)
                    if(attach.files != 1){
                        for(f2a in c(attach.files)){
                            rcouchutils::couch.attach(trackingdb,cdb.wimid,f2a)
                        }
                    }
                }
            }else{
                errdoc <- paste(df.wim.amelia$code,
                                'message',df.wim.amelia$message)
                print(paste("amelia not happy:",errdoc))
                rcouchutils::couch.set.state(year=year,
                                             id=cdb.wimid,
                                             doc=list('imputed'=
                                                          paste('error:',
                                                                errdoc)),
                                             db=trackingdb)
            }
        }

    }

    returnval
}

##' upload plots to couchdb
##'
##' pass in a list of files, and they will be uploaded as attachments
##' to the correct document in the tracking db
##'
##' @title upload.plots.couchdb
##' @param wim.site the WIM site id
##' @param direction the direction for this data
##' @param year the year
##' @param imagepath where are the images.  Will be used if you don't
##' pass in a list of files, otherwise will be ignored
##' @param trackingdb defaults to the usual "vdsdata\%2ftracking"
##' @param files.to.attach a list of files to attach to couchdb.
##'
##' If empty or if left as default value, will look through imagepath
##' and will upload all of the files found that match the pattern for
##' this wim site id, direction, and year.
##'
##' @return just falls of the end and exits.  La dee da.  Run this
##' strictly for the side effect of attaching files to the document in
##' the couchdb database
##' @author James E. Marca
upload.plots.couchdb <- function(wim.site
                                 ,direction
                                 ,year
                                 ,imagepath
                                 ,trackingdb='vdsdata%2ftracking'
                                ,files.to.attach=list()){
    cdb.wimid <- paste('wim',wim.site,direction,sep='.')

    if(length(files.to.attach) ==  0){
        file.pattern <- paste(wim.site,direction,year,'.*png',sep='_')
        file.path <- paste(paste(imagepath,direction,'/',sep=''),sep='')
        files.to.attach <- dir(file.path,
                               pattern=paste("^",file.pattern,sep=''),
                               full.names=TRUE)
    }
    for(f2a in files.to.attach){
        rcouchutils::couch.attach(db=trackingdb
                                 ,docname=cdb.wimid
                                 ,attfile=f2a
                                  )
    }
}

#' Plot WIM data and save the resulting plots to the files system and
#' CouchDB tracking database
#'
#' This is more or less a generic function to plot data either before
#' or after running Amelia.  Similar to the VDS version, but this one
#' works for WIM data, but it doesn't care if imputations have been
#' done or not, so indicate so by including a note in the fileprefix
#' parameter
#'
#'
#' @param df.merged the dataframe to plot
#' @param site_no the WIM site number
#' @param direction the direction of flow at the site
#' @param year the year
#' @param fileprefix helps name the output file, and also to find it.
#' By default the plot file will be named via the pattern
#'
#'     imagefileprefix <- paste(site_no,direction,year,sep='_')
#'
#' But if you include the fileprefix parameter, then the image file
#' naming will have the pattern
#'
#'     imagefileprefix <- paste(site_no,direction,year,fileprefix,sep='_')
#'
#' So you can add something like "imputed" to the file name to
#' differentiate the imputed plots from the input data plots.
#' @param subhead Written on the plot
#' @param force.plot defaults to FALSE.  If FALSE, and a file exists
#' @param trackingdb defaults to 'vdsdata\%2ftracking' for checking if
#' plots already done
#' @return files.to.attach the files that you need to send off to
#' couchdb tracking database.
#' @export
plot_wim.data  <- function(df.merged,site_no,direction,year,fileprefix=NULL,subhead='\npost imputation',force.plot=FALSE,trackingdb){
    cdb.wimid <- paste('wim',site_no,direction,sep='.')
    if(!force.plot){
        testfile <- paste(site_no,direction,year,sep='_')
        if(!is.null(fileprefix)){
            testfile <- paste(testfile,fileprefix,sep='_')
        }
        testfile <- paste(testfile,'006.png',sep='_')
        have.plot <- rcouchutils::couch.has.attachment(trackingdb
                                                      ,docname = cdb.wimid
                                                      ,testfile)
        if(have.plot){
            return (1)
        }
    }
    ## print('need to make plots')
    varnames <- names(df.merged)
    ## make some diagnostic plots

    ## set up a reconfigured dataframe
    recoded <- recode.df.wim( df.merged )

    nh_spds <- recoded$nh_speed/recoded$not_heavyheavy
    ## for coloring
    daymidpoint <- 12
    nh_midpoint <- mean(recoded$not_heavyheavy,na.rm=TRUE)
    hh_midpoint <- mean(recoded$heavyheavy,na.rm=TRUE)
    nh_spds_iles <- quantile(nh_spds,
                      probs=c(0.01,0.25,0.5,0.75,0.99),na.rm=TRUE)
    hhspd_midpoint <- median(recoded$hh_speed/recoded$heavyheavy,na.rm=TRUE)

    savepath <- 'images'
    if(!file.exists(savepath)){dir.create(savepath)}
    savepath <- paste(savepath,site_no,sep='/')
    if(!file.exists(savepath)){dir.create(savepath)}
    imagefileprefix <- paste(site_no,year,sep='_')
    if(direction != ''){
        savepath <- paste(savepath,direction,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        imagefileprefix <- paste(site_no,direction,year,sep='_')
    }
    if(!is.null(fileprefix) && fileprefix != ''){
        imagefileprefix <- paste(imagefileprefix,fileprefix,sep='_')
    }

    imagefilename <- paste(savepath,
                           paste(imagefileprefix,'%03d.png',sep='_'),sep='/')

    ## print(paste('plotting to',imagefilename))

    numlanes <- length(levels(recoded$lane))
    plotheight = 400 * numlanes
    png(filename = imagefilename, width=1600, height=plotheight, bg="transparent",pointsize=24)

    p <- ggplot2::ggplot(recoded)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot not heavy heavy duty truck hourly counts in each lane, by time of day and day of week, for",year,"at site",site_no,direction,subhead),
                           x="time of day",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = tod
                    ,y = not_heavyheavy
                    ,colour= heavyheavy
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane~day)

    q <- q + ggplot2::scale_color_gradient2('heavy heavy trucks',
                                            midpoint=hh_midpoint,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot not heavy heavy duty truck hourly counts in each lane, for",year,"at site",site_no,direction,subhead),
                           x="date",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = ts
                    ,y = not_heavyheavy
                    ,colour= heavyheavy
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane ~ .)

    q <- q + ggplot2::scale_color_gradient2('heavy heavy trucks',
                                            midpoint=hh_midpoint,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot heavy heavy duty truck hourly counts in each lane, by time of day and day of week, for",year,"at site",site_no,direction,subhead),
                           x="time of day",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = tod
                    ,y = heavyheavy
                    ,colour= not_heavyheavy
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane~day)

    q <- q + ggplot2::scale_color_gradient2('not heavy heavy trucks',
                                            midpoint=nh_midpoint,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot heavy heavy duty truck hourly counts in each lane, for",year,"at site",site_no,direction,subhead),
                           x="date",
                           y="hourly counts per lane"))
    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = ts
                    ,y = heavyheavy
                    ,colour= not_heavyheavy
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane ~ .)
    q <- q + ggplot2::scale_color_gradient2('not heavy heavy trucks',
                                            midpoint=nh_midpoint,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))
    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot count of all vehciles from WIM summary reports, in each lane by time of day and day of week, for",year,"at site",site_no,direction,subhead),
                           x="time of day",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = tod
                    ,y = count_all_veh_speed
                    ,colour= wgt_spd_all_veh_speed/count_all_veh_speed
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane~day)

    q <- q + ggplot2::scale_color_gradient2('average speed',
                                            midpoint=65,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))

    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot hourly speeds of all vehciles from WIM summary reports, in each lane by time of day and day of week, for",year,"at site",site_no,direction,subhead),
                           x="time of day",
                           y="hourly speed per lane, miles per hour"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = tod
                    ,y = wgt_spd_all_veh_speed/count_all_veh_speed
                    ,colour=count_all_veh_speed )
       ,alpha=0.7
        )
    q <- q + ggplot2::facet_grid(lane~day)

    q <- q + ggplot2::scale_color_gradient2('vehicle counts',
                                            midpoint=mean(recoded$count_all_veh_speed,na.rm=TRUE),
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))

    print(q)

    dev.off()

    files.to.attach <- dir(savepath,pattern=paste(imagefileprefix,'0',sep='_'),full.names=TRUE)

    files.to.attach
}
##' recode the wim data for plotting
##'
##' @title recode.df.wim
##' @param df the dataframe of data
##' @return a recoded dataframe, using melt and all that
##' @author James E. Marca
recode.df.wim <- function( df ){
    varnames <- names(df)
    keepvars <- grep('_(r|l)\\d+$',x=varnames,perl=TRUE,value=TRUE)
    unlaned_vars <- unique(substr(keepvars,start=1,stop=nchar(keepvars)-3))

    melded <- NA
    for(i in 1:length(unlaned_vars)){
        measure.vars <- grep(pattern=paste('^',unlaned_vars[i],sep=''),x=varnames,value=TRUE)
        melt_1 <- reshape2::melt(data=df,
                                 measure.vars=measure.vars,
                                 id.vars=c('ts','tod','day')
                                ,variable.name='lane'
                                ,value.name=unlaned_vars[i])
        start_lane_part <- nchar(levels(melt_1$lane)[1])-1
        end_lane_part   <- nchar(levels(melt_1$lane)[1])

        melt_1$lane <- as.factor(substr(melt_1$lane,
                                        start_lane_part,
                                        end_lane_part))

        if(i == 1){
            melded <- melt_1
        }else{
            melded <- merge(x=melded,y=melt_1,all=TRUE)
        }
    }
    ## some useful factor things
    melded$day <- factor(melded$day,
                         labels=c('Su','Mo','Tu','We','Th','Fr','Sa'))

    ## lanes?

    lanes <- levels(as.factor(melded$lane))

    ## this is a hack, is stupid, and is wrong
    ## Often with WIM sites, there are in fact multiple right lanes and no data in the left lane
    ## lane.names <- c("left","right")
    ## if(length(lanes)==1){
    ##     lane.names <- c("right")
    ## }else{
    ##     numbering <- length(lanes)
    ##     if(length(lanes)>2){
    ##         for(i in 3:numbering){
    ##             lane.names[i]=paste("lane",(numbering-i+2))
    ##         }
    ##         ## a little switcheroo here
    ##         lanes <- c(lanes[1],rev(lanes[-1]))
    ##         lane.names <- c(lane.names[1],rev(lane.names[-1]))
    ##     }
    ## }

    ## instead, iterate over each "level" of lanes.  if it has an R, then make it a right lane.  If it has an l1, then make it lane 1.
    ##
    ## I guess two passes will work.  One to suss out left right lanes, max right lane number, second to assign.
    maxright <- 0
    haveleft <- FALSE
    for (l in lanes){

    }

    melded$lane <- factor(melded$lane,
                          levels=lanes,
                          labels=lane.names)

    melded

}
