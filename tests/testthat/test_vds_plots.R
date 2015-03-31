source('../../lib/get.medianed.amelia.vds.R',chdir=TRUE)
library(testthat)

test_that("plotting imputed data code works okay",{

    file <- './files/718204_ML_2012.120.imputed.RData'
    res <- load(file)
    expect_that(res,equals('df.vds.agg.imputed'))

    df.merged <- condense.amelia.output(df.vds.agg.imputed)
    expect_that(dim(df.merged),equals(c(8784,12)))

    varnames <- names(df.merged)
    expect_that(varnames,equals(c("ts", "nl1","nr3","nr2","nr1",
                                  "ol1", "or3","or2","or1",
                                  "obs_count","tod","day")))

    expect_that(min(df.merged$nl1),equals(0.0))
    expect_that(median(df.merged$nl1),equals(1331.2886693577))
    expect_that(mean(df.merged$nl1),equals(1224.8003974521))
    expect_that(max(df.merged$nl1),equals(2362.7500629902))

### that should be good enough to verify that condense hasn't changed
### its spots

    twerked.df <- recode.df.vds(df.merged)

    expect_that(dim(twerked.df),equals(c(35136,7)))
    expect_that(names(twerked.df),equals(c("ts","tod","day",
                                          "obs_count","lane","volume",
                                          "occupancy")))

    expect_that(levels(twerked.df$lane),equals(c( "left",   "lane 2", "lane 3", "right" )))

    files.to.couch <- plot.vds.data(df.merged,718204,2012,'imputed','\npost imputation',force.plot=TRUE)

    expect_that(files.to.couch,equals(
        c( "images/718204/718204_2012_imputed_001.png",
          "images/718204/718204_2012_imputed_002.png",
          "images/718204/718204_2012_imputed_003.png",
          "images/718204/718204_2012_imputed_004.png"
          )))

    ## should also md5 check the dumped images?
})

test_that("plotting raw data code works okay",{

    file <- './files/718204_ML_2012.df.2012.RData'
    df <- load.file(f=file,fname='718204_ML_2012',year=2012,path='.')

    expect_that(dim(df),equals(c(828422,9)))
    expect_that(names(df),equals(c("nl1","ol1","nr3","or3",
                                   "nr2","or2","nr1","or1",
                                   "ts" )))

    ts <- df$ts
    df$ts <- NULL
    ## aggregate up to an hour?
    df.agg <- vds.aggregate(df,ts,seconds=3600)

    expect_that(dim(df.agg),equals(c(8784,12)))
    expect_that(length(df.agg$nl1[is.na(df.agg$nl1)]),equals(2750))

    expect_that(names(df.agg),
                equals(c("ts", "nl1","nr3","nr2","nr1",
                         "ol1", "or3","or2","or1",
                         "obs_count","tod","day")))

    expect_that(min(df.agg$nl1,na.rm=TRUE),equals(0.0))
    expect_that(median(df.agg$nl1,na.rm=TRUE),equals(1286.0))
    expect_that(mean(df.agg$nl1,na.rm=TRUE),equals(1196.2996353994))
    expect_that(max(df.agg$nl1,na.rm=TRUE),equals(2349))

### that should be good enough to verify that condense hasn't changed
### its spots

    twerked.df <- recode.df.vds(df.agg)

    expect_that(dim(twerked.df),equals(c(35136,7)))
    expect_that(names(twerked.df),equals(c("ts","tod","day",
                                          "obs_count","lane","volume",
                                          "occupancy")))

    expect_that(levels(twerked.df$lane),equals(c( "left",   "lane 2", "lane 3", "right" )))

    files.to.couch <- plot.vds.data(df.agg,718204,2012,'raw','\npre imputation',force.plot=TRUE)

    expect_that(files.to.couch,equals(
        c( "images/718204/718204_2012_raw_001.png",
          "images/718204/718204_2012_raw_002.png",
          "images/718204/718204_2012_raw_003.png",
          "images/718204/718204_2012_raw_004.png"
          )))

    ## should also md5 check the dumped images?
})

test_that("plotting raw from a site with two lanes works okay",{

    file <- './files/1211682_ML_2012.df.2012.RData'
    df <- load.file(f=file,fname='1211682_ML_2012',year=2012,path='.')

    expect_that(dim(df),equals(c(1013499,5)))
    expect_that(names(df),equals(c("nl1","ol1","nr1","or1",
                                   "ts" )))

    ts <- df$ts
    df$ts <- NULL
    ## aggregate up to an hour?
    df.agg <- vds.aggregate(df,ts,seconds=3600)

    expect_that(dim(df.agg),equals(c(8784,8)))
    expect_that(length(df.agg$nl1[is.na(df.agg$nl1)]),equals(1209))

    expect_that(names(df.agg),
                equals(c("ts", "nl1","nr1",
                         "ol1", "or1",
                         "obs_count","tod","day")))

    expect_that(min(df.agg$nl1,na.rm=TRUE),equals(0.0))
    expect_that(median(df.agg$nl1,na.rm=TRUE),equals(210.0))
    expect_that(mean(df.agg$nl1,na.rm=TRUE),equals(267.6293069307))
    expect_that(max(df.agg$nl1,na.rm=TRUE),equals(1567))

### that should be good enough to verify that condense hasn't changed
### its spots

    twerked.df <- recode.df.vds(df.agg)

    expect_that(dim(twerked.df),equals(c(17568,7)))
    expect_that(names(twerked.df),equals(c("ts","tod","day",
                                          "obs_count","lane","volume",
                                          "occupancy")))

    expect_that(levels(twerked.df$lane),equals(c( "left", "right" )))

    files.to.couch <- plot.vds.data(df.agg,1211682,2012,'raw','\npre imputation',force.plot=TRUE)

    expect_that(files.to.couch,equals(
        c("images/1211682/1211682_2012_raw_001.png",
          "images/1211682/1211682_2012_raw_002.png",
          "images/1211682/1211682_2012_raw_003.png",
          "images/1211682/1211682_2012_raw_004.png"
          )))

    ## should also md5 check the dumped images?
})

test_that("triggering native amelia plots works okay",{

    file <- './files/1211682_ML_2012.120.imputed.RData'
    res <- load(file)
    expect_that(res,equals('df.vds.agg.imputed'))

    vars = c("nl1","ol1","nr1","or1")
    files.to.couch <- plot.amelia.plots(df.vds.agg.imputed,vars=vars,1211682,2012,force.plot=TRUE)

    expect_that(files.to.couch,equals(
        c("images/1211682/1211682_2012_amelia_001.png"
          )))

    ## should also md5 check the dumped images?
})

test_that("bail out rejected amelia run",{

    file <- './files/1215965_ML_2012.120.imputed.RData'
    res <- load(file)
    expect_that(res,equals('reject'))
    expect_that(res,is_a('data.frame'))


    expect_that(get.and.plot.vds.amelia(pair = 1215965,year=2012,doplots = TRUE,
                                        remote=FALSE,path='.',force.plot = TRUE
                                        ),is_null())


})

## actually, this detector really is pretty broken
## test_that("bail out rejected amelia run",{
##     file <- '/data/backup/pems/breakup//D12/241/N/S_OF_CHIQUITA/1215855_ML_2012.df.2012.RData'
##     df <- load.file(f=file,fname='1215855_ML_2012',year=2012,path='/data/backup/pems/breakup')
##     ts <- df$ts
##     df$ts <- NULL
##     ## aggregate up to an hour?
##     df.agg <- vds.aggregate(df,ts,seconds=120)
##     twerked.df <- recode.df.vds(df.agg)
##     files.to.couch <- plot.vds.data(df.agg,1215965,2012,'raw','\npre imputation',force.plot=TRUE)
## })
