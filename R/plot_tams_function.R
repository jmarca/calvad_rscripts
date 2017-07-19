
#' Plot TAMS data and save the resulting plots to the files system and
#' CouchDB tracking database
#'
#' This is more or less a generic function to plot data either before
#' or after running Amelia.  Similar to the VDS version, but this one
#' works for TAMS data, but it doesn't care if imputations have been
#' done or not, so indicate so by including a note in the fileprefix
#' parameter
#'
#'
#' @param df the dataframe to plot
#' @param site_no the TAMS site number
#' @param direction the direction of flow at the site
#' @param year the year
#' @param lanes.count the number of lanes at the site
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
#' @param force.plot defaults to FALSE.  If FALSE, and a file exists, abort
#' @param trackingdb defaults to 'vdsdata\%2ftracking' for checking if
#' plots already done
#' @param tams.path where to save the files.  defaults to current working directory
#' @return files.to.attach the files that you need to send off to
#' couchdb tracking database.
#' @export
plot_tams.data  <- function(df,
                           site_no,
                           direction,
                           year,
                           lanes.count,
                           fileprefix=NULL,
                           subhead='post imputation',
                           force.plot=FALSE,
                           trackingdb,
                           tams.path='.'
                           ){
    cdb.tamsid <- paste('tams',site_no,direction,sep='.')
    if(!force.plot){
        testfile <- paste(site_no,direction,year,sep='_')
        if(!is.null(fileprefix)){
            testfile <- paste(testfile,fileprefix,sep='_')
        }
        testfile <- paste(testfile,'006.png',sep='_')
        have.plot <- rcouchutils::couch.has.attachment(trackingdb
                                                      ,docname = cdb.tamsid
                                                      ,testfile)
        if(have.plot){
            return (1)
        }
    }
    ## print('need to make plots')
    varnames <- names(df)
    ## make some diagnostic plots

    ## set up a reconfigured dataframe
    recoded <- recode.df.tams( df,lanes.count)

    ## for coloring
    daymidpoint <- 12
    n_midpoint <- mean(recoded$n,na.rm=TRUE)
    nh_midpoint <- mean(recoded$not_heavyheavy,na.rm=TRUE)
    hh_midpoint <- mean(recoded$heavyheavy,na.rm=TRUE)

    imagefileprefix <- paste(site_no,year,sep='_')

    savepath <- plot_path(tams.path = tams.path
                         ,year = year
                         ,site_no = site_no
                         ,direction = direction
                         ,makedir = TRUE)

    if(direction != ''){
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
        ggplot2::labs(list(title=paste("Scatterplot of hourly vehicle counts (all vehicles) in each lane, by time of day and day of week, for",year,"at site",site_no,direction),
                           subtitle=subhead,
                           x="time of day",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = tod
                    ,y = n
                    ,colour= n
                     )
       ,alpha=0.5
        )
    q <- q + ggplot2::facet_grid(lane~day)

    q <- q + ggplot2::scale_color_gradient2('all vehicles',
                                            midpoint=n_midpoint,
                                            high=("blue"),
                                            mid=("red"),
                                            low=("yellow"))
    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot hourly vehicle counts (all vehicles) in each lane, for",year,"at site",site_no,direction),
                           subtitle=paste(subhead,'colored by heavy heavy truck count'),
                           x="date",
                           y="hourly counts per lane"))

    q <- q + ggplot2::geom_point(
        ggplot2::aes(x = ts
                    ,y = n
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

    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)

    ## not heavy heavy


    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot not heavy heavy duty truck hourly counts in each lane, by time of day and day of week, for",year,"at site",site_no,direction),
                           subtitle=subhead,
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
    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot not heavy heavy duty truck hourly counts in each lane, for",year,"at site",site_no,direction),
                           subtitle=subhead,
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

    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot heavy heavy duty truck hourly counts in each lane, by time of day and day of week, for",year,"at site",site_no,direction),
                           subtitle=subhead,
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

    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)

    q <- p +
        ggplot2::labs(list(title=paste("Scatterplot heavy heavy duty truck hourly counts in each lane, for",year,"at site",site_no,direction),
                           subtitle=subhead,
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
    q <- q + ggplot2::theme(plot.title = ggplot2::element_text(hjust = 0.5),plot.subtitle = ggplot2::element_text(hjust = 0.5))
    print(q)


    dev.off()

    files.to.attach <- dir(savepath,pattern=paste(imagefileprefix,'0',sep='_'),full.names=TRUE,all.files=TRUE)

    files.to.attach
}
##' Create the output directory for TAMS data plots
##'
##' Given details, this function will create a canonical output path.
##' @title plot_path
##' @param tams.path the starting point for the directory creation process
##' @param year the year
##' @param site_no the TAMS site number
##' @param direction the direction, defaults to '' (no specific direction)
##' @param makedir whether or not to create missing directories; defaults to TRUE
##' @return a string representing the root directory to write image files
##' @author James E. Marca
##' @export
plot_path <- function(tams.path,year,site_no,direction='',makedir=TRUE){
    if(makedir && !file.exists(tams.path)){dir.create(tams.path)}
    savepath <- paste(tams.path,year,sep='/')
    if(makedir && !file.exists(savepath)){dir.create(savepath)}
    savepath <- paste(savepath,site_no,sep='/')
    if(makedir && !file.exists(savepath)){dir.create(savepath)}
    if(direction != ''){
        savepath <- paste(savepath,direction,sep='/')
        if(makedir && !file.exists(savepath)){dir.create(savepath)}
    }
    savepath <- paste(savepath,'images',sep='/')
    if(makedir && !file.exists(savepath)){dir.create(savepath)}
    savepath
}


##' recode the tams data for plotting
##'
##' @title recode.df.tams
##' @param df the dataframe of data
##' @param lanes.count the total number of lanes at the site.  for example, 5
##' @return a recoded dataframe, using melt and all that
##' @author James E. Marca
recode.df.tams <- function( df,lanes.count ){
    varnames <- names(df)
    keepvars <- grep('_(r|l)\\d+$',x=varnames,perl=TRUE,value=TRUE)
    unlaned_vars <- unique(substr(keepvars,start=1,stop=nchar(keepvars)-3))

    melded <- NA
    for(i in 1:length(unlaned_vars)){
        measure.vars <- grep(pattern=paste('^',unlaned_vars[i],'_',sep=''),x=varnames,value=TRUE)
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
    ## the lanes for a TAMS site might not include all lanes.  For
    ## example, site 7005 has 2 lanes of data (4,5), missing lanes 1,2,3
    ##
    lanes <- calvadrscripts::vds.lane.numbers(lanes.count,'')

    rightlanes <- NULL
    rightlabels <- NULL
    leftlane <- c('l1')
    leftlabel <- c('left')
    for(lane in  rev(sort(lanes))){
        if(lane == 'l1'){
            ## last time through no matter what
            next
        }
        ## but there might not be a left lane...
        rightlanes <- c(rightlanes,lane)
        if(lane == 'r1'){
            rightlabels <- c(rightlabels,'right')
        }else{
            rightlabels <- c(rightlabels,paste('lane',length(rightlabels)+2))
        }
    }

    melded$lane <- factor(melded$lane,
                          levels=c('l1',rightlanes),
                          labels=c('left',rightlabels))

    melded

}
