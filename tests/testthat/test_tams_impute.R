config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))


tams.site <- 7005
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
## note that when developing this in the R interactive terminal, then
## yeah, this path should be set as tests/testthat/files.  But when
## running under the testing framework, it should be relative to
## tests/testthat, or just 'files'
## tams.path <- 'tests/testthat/files'
tams.path <- 'files'

## make sure no artifacts from prior tests
testthat::test_that("load data, plot raw, impute, and plot imputed all work", {

    year <- 2016
    tams.data.path <- paste(tams.path,year,tams.site,sep='/')
    parts <- c('tams','impute')
    result <- rcouchutils::couch.makedb(parts)
    testthat::expect_equal(result$ok,TRUE)

    hour <- 3600

    drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_rdatas)>0) {
        unlink(drop_rdatas)
    }
    drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_pngs)>0) {
        unlink(drop_pngs)
    }

    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'E',tams.path)
    testthat::expect_equal(load.df,'todo')
    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'W',tams.path)
    testthat::expect_equal(load.df,'todo')

    load.list <- calvadrscripts::load.tams.from.fs(tams.site,year,tams.path,parts)
    testthat::expect_equal(load.list,'todo')

    ## basically a cut and paste from process.tams.site so that I can
    ## prove each step is working (in case low level code gets "improved")


    tams.data <- calvadrscripts::load.tams.from.csv(tams.site=tams.site,
                                    year=year,
                                    tams.path=tams.path)

    tams.data <- clean.tams.csv.lanes(tams.data)
    site.lanes <- max(tams.data$lane)
    testthat::expect_equal(site.lanes,5)

    tams.data <- trim.to.year(tams.data,year)
    testthat::expect_equal(dim(tams.data),c(4636452, 11))

    tams.data <- tams.recode.lane_dir(tams.data)
    testthat::expect_equal(sort(levels(as.factor(tams.data$lane_dir)))
                           ,c("E","W"))

    tams.data <- tams.recode.lanes(tams.data)
    testthat::expect_equal(sort(levels(as.factor(tams.data$lane)))
                          ,c("r1", "r2"))

    bad_hrs_set <- c()
    directions <-  levels(as.factor(tams.data$lane_dir))
    direction <- "E"
    dir_idx <- tams.data$lane_dir == direction
    lanes <- levels(as.factor(tams.data[dir_idx,]$lane))
    testthat::expect_equal(sort(lanes)
                          ,c("r1", "r2"))
    l <- "r1"
    dir_idx <- tams.data$lane_dir == direction
    lane_dir_idx <- tams.data$lane == l & dir_idx

    tams.data[lane_dir_idx,'timestamp_full']  <- date_rollover_bug(tams.data[lane_dir_idx,]$timestamp_full)
    tams.data[lane_dir_idx,'hrly'] <-
        as.numeric(
            trunc(
                as.POSIXct(
                    tams.data[lane_dir_idx,]$timestamp_full
                   ,tz='UTC')
               ,"hours")
        )
    testthat::expect_equal(length(levels(as.factor(tams.data[lane_dir_idx,]$hrly)))
                           ,1378)
    bad_hrs <- timestamp_insanity_fix(tams.data[lane_dir_idx,])
    bad_hrs_set <- union(bad_hrs_set,bad_hrs)
    testthat::expect_equal(length(bad_hrs_set),34)

    keepers <- ! is.element(tams.data[lane_dir_idx,]$hrly,bad_hrs)
    drops_num <- length(keepers[!keepers])
    prior_num <- length(keepers)

    testthat::expect_equal(drops_num,16036)
    testthat::expect_equal(prior_num,1382722)

    tams.data[lane_dir_idx,'keep'] <- FALSE
    tams.data[lane_dir_idx,][keepers,]$keep <- TRUE
    tams.data <- tams.extra.vars(tams.data)

    df_hourly <- reshape.tams.from.csv.by.dir.by.lane(
        tams.data[lane_dir_idx,],l
    )
    testthat::expect_equal(dim(df_hourly),c(1378, 4))
    testthat::expect_equal( sort(names(df_hourly))
                          ,c("heavyheavy_r1","n_r1","not_heavyheavy_r1","ts"))

    df_hourly$ts <- as.POSIXct(df_hourly$ts,origin = "1970-01-01", tz = "UTC")
    df_hourly$ts <- trunc(df_hourly$ts,units='hours')
    df_hourly$marker <- TRUE
    tams.data.hr.lane <- list() ## list for output results
    tams.data.hr.lane[[l]] <- df_hourly

    mints <- min(tams.data.hr.lane[[1]]$ts)
    maxts <- max(tams.data.hr.lane[[1]]$ts)

    all.ts <- seq(mints,maxts,by=hour)
    testthat::expect_equal(length(all.ts),1620)

    df.return <- tibble::tibble(ts=all.ts,marker=FALSE)
    ## make ts posixlt to enable merge below
    df.return$ts <- as.POSIXlt(df.return$ts)
    testthat::expect_equal(dim(df.return),c(1620,2))
    ## fake merge of lanes to full timeline
    tams.data.hr.lane[[l]]$marker <- TRUE
    df.return <- merge(df.return,tams.data.hr.lane[[l]],by=c('ts'),all=TRUE)
    testthat::expect_equal(dim(df.return),c(1620,6))


    df.return$marker <- df.return$marker.x | df.return$marker.y
    df.return$marker.x <- NULL
    df.return$marker.y <- NULL
    testthat::expect_equal(dim(df.return),c(1620,5))

    for(col in names(df.return)){
        navalues <- is.na(df.return[,col]) & !is.na(df.return$marker)
        if(any(navalues)){
            df.return[navalues,col] <- 0
        }
    }
    df.return$marker <- NULL
    df.return$ts <- as.POSIXct(df.return$ts)
    df.return <- add.time.of.day(df.return)
    hrly <- as.numeric(trunc(df.return$ts,"hours"))
    keepers <- ! is.element(hrly,bad_hrs_set)

    testthat::expect_equal(dim(df.return),c(1620,7))

    ## clean up for next test
    result <- rcouchutils::couch.deletedb(parts)
    testthat::expect_equal(result$ok,TRUE)
})

testthat::test_that("process tams  site also works okay",{
    year <- 2017 ## note 2017 data is horrible, will hit max iterations in Amelia
    tams.data.path <- paste(tams.path,year,tams.site,sep='/')
    parts <- c('tams','process_site')
    result <- rcouchutils::couch.makedb(parts)
    testthat::expect_equal(result$ok,TRUE)

    drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_rdatas)>0) {
        unlink(drop_rdatas)
    }
    drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_pngs)>0) {
        unlink(drop_pngs)
    }

    list.df.tams.amelia <- process.tams.site(tams.site=tams.site,
                                             year=year,
                                             seconds=seconds,
                                             preplot=TRUE,
                                             postplot=TRUE,
                                             force.plot=FALSE,
                                             tams.path=tams.path,
                                             trackingdb=parts
                                           )
    testthat::expect_type(list.df.tams.amelia,'list')
    directions <- names(list.df.tams.amelia)
    testthat::expect_equal(sort(directions),c('E','W'))
    for(direction in directions){
        testthat::expect_equal(list.df.tams.amelia[[direction]]$code,1)
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        doc <- rcouchutils::couch.get(parts,cdb.tamsid)
        attachments <- doc[['_attachments']]
        testthat::expect_type(attachments,'list')
        ## print(sort(names(attachments)))
        plotsname_stub <- paste('ameliaplots_',year,'_00',sep='')
        testthat::expect_equal(sort(names(attachments)),
            c(paste(tams.site,direction,year,
                    c(rep('imputed',6),rep('raw',6)),
                    c("001.png",
                      "002.png",
                      "003.png",
                      "004.png",
                      "005.png",
                      "006.png"),
                    sep='_')
             ,paste(plotsname_stub,c(1,2),'.png',sep='')
              )
            )
    }

    ## also verify that doing it again (using RData) works fine
    list.df.tams.amelia.2 <- process.tams.site(tams.site=tams.site,
                                               year=year,
                                               seconds=seconds,
                                               preplot=FALSE,
                                               postplot=TRUE,
                                               force.plot=TRUE,
                                               impute = TRUE,
                                               tams.path=tams.path,
                                               trackingdb=parts
                                               )

    ## also verify that doing it again (using RData) works fine
    list.df.tams.amelia.3 <- process.tams.site(tams.site=tams.site,
                                               year=year,
                                               seconds=seconds,
                                               preplot=FALSE,
                                               postplot=TRUE,
                                               force.plot=TRUE,
                                               impute = FALSE,
                                               tams.path=tams.path,
                                               trackingdb=parts
                                               )

    testthat::expect_equal(list.df.tams.amelia.3
                           ,list.df.tams.amelia.2)

    result <- rcouchutils::couch.deletedb(parts)
    testthat::expect_equal(result$ok,TRUE)
})


testthat::test_that("process tams site with no data fails gracefully",{
    tams.site <- 7
    year <- 2017 ## note 2017 data is horrible, will hit max iterations in Amelia
    tams.data.path <- paste(tams.path,year,tams.site,sep='/')
    parts <- c('tams','no_data_site')
    result <- rcouchutils::couch.makedb(parts)
    testthat::expect_equal(result$ok,TRUE)

    list.df.tams.amelia <- process.tams.site(tams.site=tams.site,
                                             year=year,
                                             seconds=seconds,
                                             preplot=TRUE,
                                             postplot=TRUE,
                                             force.plot=FALSE,
                                             tams.path=tams.path,
                                             trackingdb=parts
                                           )


    testthat::expect_type(list.df.tams.amelia,'list')
    testthat::expect_length(list.df.tams.amelia,0)

    for(tdp in tams.data.path){
        drop_rdatas <- dir(tdp,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
        if(length(drop_rdatas)>0) {
            unlink(drop_rdatas)
        }
        drop_pngs <- dir(tdp,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
        if(length(drop_pngs)>0) {
            unlink(drop_pngs)
        }
    }

    result <- rcouchutils::couch.deletedb(parts)
    testthat::expect_equal(result$ok,TRUE)
})


year <- c(2016,2017) ## note 2017 data is horrible, will hit max iterations in Amelia
tams.data.path <- paste(tams.path,year,tams.site,sep='/')

for(tdp in tams.data.path){
    drop_rdatas <- dir(tdp,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_rdatas)>0) {
        unlink(drop_rdatas)
    }
    drop_pngs <- dir(tdp,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_pngs)>0) {
        unlink(drop_pngs)
    }
}
