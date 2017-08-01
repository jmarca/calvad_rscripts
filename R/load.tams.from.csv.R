##' Get TAMS dataframe from a saved RData file
##'
##' Given the TAMS site number, the year, and the direction, will get
##' the correct RData file from some directory under the path
##' parameter.  If it exists, will return the reconstituted dataframe.
##' If not, not.
##'
##' @title load.tams.from.csv
##' @param tams.site the TAMS site number
##' @param year the year
##' @param direction the direction
##' @param tams.path the root path in the file system to start looking
##'     for the RData files.  The code will add in the year, then the
##'     site number, then the direction, so that the file can be found
##'     in the expected place
##' @param filename.pattern the pattern to use when searching.  Will
##'     look for tams.agg.RData by default, but if, say, you want to
##'     load the imputation output file, then pass in "imputed.RData"
##'     or similar
##' @return a dataframe with the raw, unimputed data, or NULL
##' @author James E. Marca
##' @export
##'
load.tams.from.csv <- function(tams.site,year,direction,
                               tams.path='/data/backup/tams',
                               filename.pattern=''){
    if(filename.pattern == ''){
        filename.pattern <- paste('^',tams.site,'_',year,'.*\\.(csv|csv.gz)$',sep='')
    }
    isa.csv <- dir(tams.path, pattern=filename.pattern,full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
    load.result <- list()
    if(length(isa.csv)>0){
        load.result <- do.call("rbind",lapply(isa.csv,readr::read_csv,progress=FALSE))
    }
    if(length(load.result) > 0){
        matched <- load.result$detstaid == tams.site
        ## just in case something strange gets in there
        load.result <- load.result[matched,]
    }

    return (load.result)
}


##' recode the TAMS lanes to have the usual names
##'
##' @title tams.recode.lanes
##' @param df the TAMS dataframe
##' @return the modified data frame with the lane names changed
##' @author James E. Marca
tams.recode.lanes <- function(df){

  ##
  ## recode to be right lane (r1), right lane but one (r2), r3, ... and then
  ## left lane (l1)
  ##

  lanes <- max(df$lane)
  Y <- wim.lane.numbers(lanes) ## nothing unique to WIM
  df$lane <- as.factor(Y[df$lane])
  df
}

##' recode the TAMS lane_dir to have canonical N,S,E,W
##'
##' @title tams.recode.lane_dir
##' @param df the TAMS dataframe
##' @return the modified df with the lane_dir changed from numeric to letters
##' @author James E. Marca
tams.recode.lane_dir <- function(df){

    ##
    ## according to Andre, 1=N, 2=S, 3=E, 4=W
    ## see email reply in file lane_dir.txt
    ##
    canonical_direction <- c('N','S','E','W')


    df$lane_dir <- as.factor(canonical_direction[df$lane_dir])
    df

}

##' Wild craziness for timestamps in and around midnight needs fixing.
##'
##' This function stares at the data really hard and tries to clean
##' things up.
##' @title timestamp_insanity_fix
##' @param df the data frame to fix
##' @return a list of the bad hours...those that are repeated and that
##'     should be dropped (because they can't be fixed by logic alone)
##' @author James E. Marca
timestamp_insanity_fix <- function(df){

    ## make sure df is sorted by id, not time
    id_order <- order(df$sig_id)
    df <- df[id_order,]
    ## expect hrly exists and is what I want
    hrly <- df$hrly

    ## expect time to monotonically increase.
    ## that means any repeated hours is a bad thing (TM)

    ## step through the times.  If hour goes backwards, flag it as bad
    len <- length(hrly)
    time_shifts <- hrly[-1] - hrly[-len]
    hr_shifts <- c(hrly[1],hrly[-1][time_shifts != 0])
    temp_df <- tibble::tibble(hr=hr_shifts,n=1)

    sqlstatement <- paste("select min(hr) as hr",
                          ', total(1) as cnt',
                          'from temp_df group by hr order by hr',
                          sep=' ',collapse=' '
                          )
    result <- sqldf::sqldf(sqlstatement,drv="SQLite")
    dupes <- result$cnt>1

    ## keepers <- ! is.element(hrly,result$hr[dupes])
    ## keepers
    result$hr[dupes]
}

##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##'
##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##' @title off_timestamp
##' @param times the vector of times to inspect
##' @param offset positive looks for too soon a day jump, negative
##'     looks for too late a day jump
##' @return an index of the ones that are a day off.  If all false,
##'     then you are done
##' @author James E. Marca
off_timestamp <- function(times,offset=-23){
    last.idx <- length(times)
    timestamp_diff <- as.numeric(times[-1] - times[-last.idx],units="hours")
    bad_time <- NULL
    round_midnight <- NULL
    ts.lt <- as.POSIXlt(times)

    if(offset < 0){
        round_midnight <- (ts.lt$hour[-1] == 0  & ts.lt$min[-1] == 0)
        bad_time <- timestamp_diff < -23 & timestamp_diff > -25
    }else{
        round_midnight <- (ts.lt$hour[-1] == 23 & ts.lt$min[-1] == 59)
        bad_time <- timestamp_diff > 23 & timestamp_diff < 25
    }

    ten.or.less <- min(10,length(bad_time[bad_time & round_midnight]))
    print(paste('got',length(bad_time[bad_time & round_midnight]),'problem entries'))
    if(length(bad_time[bad_time & round_midnight])>0){
        print('first few erroneous entries')
        print(times[-1][bad_time & round_midnight][1:ten.or.less])
        print('entries immediately preceding bad entries')
        print(times[bad_time & round_midnight][1:ten.or.less])
    }
    bad_time & round_midnight
}

##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##'
##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##' @title day_back_timestamp
##' @param times the vector of times to inspect
##' @return an index of the ones that are a day off.  If all false,
##'     then you are done
##' @author James E. Marca
day_back_timestamp <- function(times){
    off_timestamp(times,-23)
}


##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##'
##' Do the work of calculating the timestamps affected by the day fail
##' to roll over bug
##' @title day_forward_timestamp
##' @param times the vector of times to inspect
##' @return an index of the ones that are a day off.  If all false,
##'     then you are done
day_forward_timestamp <- function(times){
    off_timestamp(times,23)
}


##' A function to fix the cases where the date doesn't increment
##' properly but the hour:minute do switch over to 00:00
##'
##' This function should be passed the times from almost pure CSV
##' data.  The only modification should be that the directions are
##' uniform (I'm going to expect all lane_dir is the same).  The
##' sequence of times will be checked to make sure that the date
##' increments properly when it should.
##'
##' This fixes both problem cases identified: that the date increments
##' slightly before midnight, and that the date does not increment
##' slightly after midnight.
##' @title date_rollover_bug
##' @param times a list of times to analyze
##' @return a corrected list of times to use instead of the passed in
##'     list
##' @author James E. Marca
date_rollover_bug <- function(times){

    ## time by lane and direction should be uniformly increasing
    ## except it doesn't always.  The only real bug is when it rolls
    ## over the end of the day, resets the hour to 00, but doesn't
    ## properly increment the date.  Sadly there might be a few of
    ## these.

    loopcount <- 0
    positive_time <- day_forward_timestamp(times)
    while(length(positive_time[positive_time]) > 0 && loopcount < 10){
        print(loopcount <- loopcount + 1)
        times[-1][positive_time] <- times[-1][positive_time] - 24 * 60 * 60
        negative_time <- day_back_timestamp(times)
        if(length(negative_time)>0){
            times[-1][negative_time] <- times[-1][negative_time] + 24 * 60 * 60
        }
        positive_time <- day_forward_timestamp(times)
    }
    negative_time <- day_back_timestamp(times)
    while(length(negative_time[negative_time])>0 && loopcount < 10){
        print(loopcount <- loopcount + 1)
        times[-1][negative_time] <- times[-1][negative_time] + 24 * 60 * 60
        negative_time <- day_back_timestamp(times)
    }
    times
}

##' reshape.tams.from.csv
##' Take tams raw CSV, and turn it into one tibble per direction
##'
##' Given a csv file, this function will process the CSV into one tibble per direction.
##'
##' @title reshape.tams.from.csv
##' @param tams.csv csv file read from csv, for example, by
##'     readr::read_csv
##' @param tams.path Where to save RData files, one per direction
##' @param year the year
##' @param trim.to.year whether to trim the data to the specified
##'     year.  defaults to true
##' @return A list of dataframs, one per direction in the original CSV
##'     data.
##' @author James E. Marca
##' @export
##'
reshape.tams.from.csv <- function(tams.csv,tams.path,year,trim.to.year=TRUE){
    ## prep for sqldf if you need postgresql
    # config <- rcouchutils::get.config()
    # sqldf_postgresql(config)

    ## make sure order is correct.  No guarantee that files are read
    ## in in sig_id ordering
    ## o <- order(tams.csv$sig_id)
    ## tams.csv <- tams.csv[o,]

    ## extract some info
    tams.site <- tams.csv$detstaid[1]
    number.lanes <- max(tams.csv$lane)

    hour <-  3600 ## seconds per hour

    ts.ct   <- as.POSIXct(tams.csv$timestamp_full,tz='UTC')

    if(trim.to.year){
        ## trim any records not in the requested year
        print(paste('trimming to year',year))
        keep.records <- (1900 + as.POSIXlt.date(ts.ct)$year) ==  year
        ## print(summary(keep.records))
        ## print('length original')
        ## print(dim(df.return))
        ## print('length keep')
        ## print(dim(df.return[keep.records,]))
        tams.csv <- tams.csv[keep.records,]
        ts.ct <- ts.ct[keep.records]
    }

    tams.csv$hrly <- as.numeric(trunc(ts.ct,"hours"))
    tams.csv <- tams.recode.lanes(tams.recode.lane_dir(tams.csv))
    tams.csv$any_vehicle <- 1
    tams.csv$heavyheavy <- 0
    tams.csv$not_heavyheavy <- 0
    tams.csv$heavyheavy[tams.csv$calvad_class == 'HHDT'] <- 1
    tams.csv$not_heavyheavy[tams.csv$calvad_class == 'NHHDT'] <- 1
    ##if(length(levels(tams.csv$lane_dir)) > 1)

    mean.var.names <- c('not_heavyheavy','heavyheavy')

    tams.by.dir <- split(tams.csv,tams.csv$lane_dir)
    directions <- names(tams.by.dir)
    bad_hrs_set <- c()

    for(direction in directions){
        print(paste('processing',tams.site,direction))
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        tams <- tams.by.dir[[direction]]
        if(length(dim(tams)) !=  2){
            ## probably have an issue here
            print(paste("no data loaded for",tams.site,direction," ---  skipping"))
            next
        }

        ## preplot prior to cleaning??
        tams.data.hr.lane <- split(tams,tams$lane)
        lanes <- names(tams.data.hr.lane)

        for (l in lanes){
            temp_df <- tams.data.hr.lane[[l]]

            ## fix the date rollover bug
            fixed_times <- date_rollover_bug(temp_df$timestamp_full)
            temp_df$timestamp_full <- fixed_times
            ts.ct   <- as.POSIXct(fixed_times,tz='UTC')
            temp_df$hrly <- as.numeric(trunc(ts.ct,"hours"))
            bad_hrs <- timestamp_insanity_fix(temp_df)
            bad_hrs_set <- union(bad_hrs_set,bad_hrs)
            keepers <- ! is.element(temp_df$hrly,bad_hrs)
            drops_num <- length(keepers[!keepers])
            prior_num <- length(keepers)
            print(paste('dropping',drops_num,'out of',prior_num,'total records (or',round(100*(drops_num/prior_num),2),'percent) due to duplicated hours'))
            temp_df <- temp_df[keepers,]

            sqlstatement2 <- paste("select min(hrly) as ts,",
                                   paste('total(',
                                         c(mean.var.names),
                                         ') as ',
                                         paste(mean.var.names,
                                               l,
                                               sep='_'),
                                         sep=' ',
                                         collapse=', '),
                                   ', total(any_vehicle) as ',
                                   paste('n',l,sep='_'),
                                   'from temp_df group by hrly',
                                   sep=' ',collapse=' '
                                   )
            df_hourly <- sqldf::sqldf(sqlstatement2,drv="SQLite")
            df_hourly$ts <- as.POSIXct(df_hourly$ts,origin = "1970-01-01", tz = "GMT")
            df_hourly$ts <- trunc(df_hourly$ts,units='hours')
            tams.data.hr.lane[[l]] <- df_hourly
        }
        print('combine lane by lane aggregates')
        ## combine lane by lane aggregates by same hour
        df.return <- tams.data.hr.lane[[1]]
        if(length(tams.data.hr.lane)>1){
            for(l in 2:length(tams.data.hr.lane)){
                df.return <- merge(df.return,tams.data.hr.lane[[l]],all=TRUE)
            }
        }
        print(dim(df.return))
        for(col in names(df.return)){
            navalues <- is.na(df.return[,col])
            if(any(navalues)){
                df.return[navalues,col] <- 0
            }
        }
        df.return$ts <- as.POSIXct(df.return$ts)
        all.ts <- seq(min(df.return$ts),max(df.return$ts),by=hour)
        ts.all.df <- tibble::tibble(ts=all.ts)
        attr(ts.all.df$ts,'tzone') <- 'UTC'
        df.return <- merge(df.return,ts.all.df,all=TRUE)



        df.return <- add.time.of.day(df.return)
        tams.by.dir[[direction]] <- df.return


    }

    ## finally, do one more pass to drop any missed bad hours
    for(direction in directions){
        df.return <- tams.by.dir[[direction]]

        hrly <- as.numeric(trunc(df.return$ts,"hours"))
        keepers <- ! is.element(hrly,bad_hrs_set)
        if(length(keepers[!keepers]) > 0){
            print(paste('second pass, dropping',length(keepers[!keepers]),'records due to duplicated hours'))
            df.return <- df.return[keepers,]
            tams.by.dir[[direction]] <- df.return
        }

        ## save tams data for next time
        filename <- make.tams.output.filename(tams.site,direction,year)
        savepath <- tams.path
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(tams.path,year,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,tams.site,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        savepath <- paste(savepath,direction,sep='/')
        if(!file.exists(savepath)){dir.create(savepath)}
        filepath <- paste(savepath,filename,sep='/')
        save(df.return,file=filepath,compress='xz')
        print(paste('saved',tams.site,direction,'to',filepath))
    }

    list(tams.by.dir,number.lanes)
}

##' Make the canonical TAMS output filename.
##'
##' Given the site number, direction, and year, create a filename that
##' can be used for saving the data.frame for that site, year,
##' direction to the filesystem.  Did this so that I can use it again
##' when fetching files.
##' @title make.tams.output.filename
##' @param tams.site the tams site
##' @param direction the direction, one of [NSEW]
##' @param year the year
##' @return a string that can be used as a filename
##' @author James E. Marca
make.tams.output.filename <- function(tams.site,direction,year){
    paste(tams.site,direction,year,'tams.agg.RData',sep='.')
}

##' Load TAMS data from a saved RData file.
##'
##' This function will use the provided site number, year, and
##' direction to look below the tams.path to find if a suitable RData
##' file already exists.  If so, it will load it and return it as a
##' data.frame.  If not, it will return the character string 'todo'.
##' @title load.tams.from.file
##' @param tams.site the tams site
##' @param year the year
##' @param direction the direction, 'N', 'S', 'E', or 'W'
##' @param tams.path the relative path to where to find TAMS files
##' @return a data frame or a string
##' @author James E. Marca
##' @export
load.tams.from.file <- function(tams.site,year,direction,tams.path){

    target.file <- make.tams.output.filename(tams.site,direction,year)
    target.file <- paste(target.file,'$',sep='')
    savepath <-  paste(tams.path,year,tams.site,sep='/')
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
