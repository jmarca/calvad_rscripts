test_that("plotting imputed data code works okay",{

    file  <- './files/718204_ML_2012.df.2012.RData'
    fname <- '718204_ML_2012'
    vds.id <- 718204
    year <- 2012
    seconds <- 120
    path <- '.'
    df <- load.file(file,fname,year,path)
    ## break out times
    ts <- df$ts
    df$ts <- NULL
    seconds=120
    df.agg <- vds.aggregate(df,ts,seconds=seconds)

    expect_that(dim(df.agg),equals(c(263520,12)))
    expect_that(length(df.agg$nl1[is.na(df.agg$nl1)]),equals(57728))

    expect_that(names(df.agg),
                equals(c("ts", "nl1","nr3","nr2","nr1",
                         "ol1", "or3","or2","or1",
                         "obs_count","tod","day")))

    ## use sprintf("%0.10f",mean(df.agg$nl1,na.rm=TRUE)) to get long
    ## decimal places
    expect_that(min(df.agg$nl1,na.rm=TRUE),equals(0.0))
    expect_that(median(df.agg$nl1,na.rm=TRUE),equals(43.0))
    expect_that(mean(df.agg$nl1,na.rm=TRUE),equals(40.7003479241))
    expect_that(max(df.agg$nl1,na.rm=TRUE),equals(101))

    lanes <- longway.guess.lanes(df)
    n.idx <- vds.lane.numbers(lanes,c("n"))
    o.idx <- vds.lane.numbers(lanes,c("o"))
    o.cols <- (1:length(names(df.agg)))[is.element(names(df.agg), o.idx)]
    o.bds.len <- length(o.cols)
    o.bds <- matrix(c(o.cols,sort( rep(c(0, 1),o.bds.len))), nrow = o.bds.len, byrow=FALSE)


    df.vds.agg.imputed <- NULL
    maxiter <-  20
    df.agg$toddow <- 24 * df.agg$day + df.agg$tod
    r <- try(
        df.vds.agg.imputed <-
            Amelia::amelia(
                df.agg,
                idvars=c('ts','toddow','obs_count'),
                ts="tod",
                splinetime=6,
                autopri=0.001,
                lags =c(n.idx,o.idx),
                leads=c(n.idx,o.idx),
                cs="day",
                intercs=TRUE,
                sqrts=c(n.idx,o.idx),

                bounds=o.bds,
                max.resample=10,
                emburn=c(2,maxiter))
        )

    expect_that(r,is_a('amelia'))
    ## expect_that(res,equals('df.vds.agg.imputed'))

    df.merged <- condense.amelia.output(df.vds.agg.imputed)
    expect_that(dim(df.merged),equals(c(8784,12)))

    varnames <- names(df.merged)
    expect_that(varnames,equals(c("ts", "nl1","nr3","nr2","nr1",
                                  "ol1", "or3","or2","or1",
                                  "obs_count","tod","day")))

    ## expect_that(min(df.merged$nl1),equals(0.0))
    ## expect_that(median(df.merged$nl1),equals(1331.2886693577))
    ## expect_that(mean(df.merged$nl1),less_than(1300))
    ## expect_that(max(df.merged$nl1),less_that(2500))

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
        c("images/718204/718204_2012_imputed_001.png",
          "images/718204/718204_2012_imputed_002.png",
          "images/718204/718204_2012_imputed_003.png",
          "images/718204/718204_2012_imputed_004.png"
          )))

    ## should also md5 check the dumped images?
})
