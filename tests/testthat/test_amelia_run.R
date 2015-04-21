test_that("plotting imputed data code works okay",{

    file  <- './files/763346_ML_2012.df.2012.RData'
    fname <- '763346_ML_2012'
    vds.id <- 763346
    year <- 2012
    seconds <- 120
    path <- '.'
    df <- load.file(file,fname,year,path)
    ## break out times
    ts <- df$ts
    df$ts <- NULL
    seconds=120
    df.agg <- vds.aggregate(df,ts,seconds=seconds)
    ## plot for comparisons.  just need to do this once
    df.agg.3600 <- vds.aggregate(df,ts,seconds=3600)
    files.to.couch <- plot_vds.data(df.agg.3600,vds.id,2012,'raw','\npre imputation',force.plot=TRUE)

    ## use sprintf("%0.10f",mean(df.agg$nl1,na.rm=TRUE)) to get long

    lanes <- longway.guess.lanes(df)
    n.idx <- vds.lane.numbers(lanes,c("n"))
    o.idx <- vds.lane.numbers(lanes,c("o"))
    o.cols <- (1:length(names(df.agg)))[is.element(names(df.agg), o.idx)]
    o.bds.len <- length(o.cols)
    o.bds <- matrix(c(o.cols,sort( rep(c(0, 1),o.bds.len))), nrow = o.bds.len, byrow=FALSE)
    

    df.vds.agg.imputed <- NULL
    maxiter <-  20

    r <- try(
        df.vds.agg.imputed <-
            Amelia::amelia(df.agg,
                   idvars=c('ts','obs_count'),
                   ts="tod",
                   splinetime=6,
                   autopri=0.001,
                   lags =c(n.idx),
                   leads=c(n.idx),
                   cs="day",
                   intercs=TRUE,
                   sqrts=n.idx,
                   bounds=o.bds,
                   max.resample=10,
                   emburn=c(2,maxiter))
        )

    expect_that(r,is_a('amelia'))
    ## expect_that(res,equals('df.vds.agg.imputed'))

    df.merged <- condense.amelia.output(df.vds.agg.imputed)
    twerked.df <- recode.df.vds(df.merged)
    files.to.couch <- plot_vds.data(df.merged,vds.id,2012,'imputed','\npost imputation',force.plot=TRUE)

    expect_that(files.to.couch,equals(
        paste("images/",vds.id,"/",vds.id,'_',year,'imputed_00',c(1,2,3,4),'.png',sep='')
        ))
})
