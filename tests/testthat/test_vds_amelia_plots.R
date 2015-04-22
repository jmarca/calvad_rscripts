config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('vds','amelia','plots')
result <- rcouchutils::couch.makedb(parts)


context('get.and.plot.vds.amelia works okay')
test_that("plotting imputed data code works okay",{
    file  <- './files/737237_ML_2012.df.2012.RData'
    fname <- '737237_ML_2012'
    vds.id <- 737237
    year <- 2012
    seconds <- 120
    path <- '.'

    df_agg <- get.and.plot.vds.amelia(
        pair=vds.id,
        year=year,
        doplots=TRUE,
        remote=FALSE,
        path=path,
        force.plot=TRUE,
        trackingdb=parts)

    expect_that(df_agg,is_a('data.frame'))
    expect_that(names(df_agg),equals(c("ts","nl1","nr1",
                                       "ol1","or1","obs_count",
                                       "tod","day")))

    expect_that(min(df_agg$nl1,na.rm=TRUE),equals(0.0))
    ## print(sprintf("%0.10f",mean(df_agg$nl1,na.rm=TRUE)))
    expect_that(mean(df_agg$nl1,na.rm=TRUE),equals(780.1367564096,tolerance = .00001))
    ## print(sprintf("%0.10f",median(df_agg$nl1,na.rm=TRUE)))
    expect_that(median(df_agg$nl1,na.rm=TRUE),equals(883))
    ## print(sprintf("%0.10f",max(df_agg$nl1,na.rm=TRUE)))
    expect_that(max(df_agg$nl1,na.rm=TRUE),equals(1861))


    plots <- paste(vds.id,year,'imputed',
                   c('001.png','002.png','003.png','004.png'),
                   sep='_')
    for(plot in plots){
        result <- rcouchutils::couch.has.attachment(db=parts,docname=vds.id,
                                                    attachment=plot)
        expect_true(result)
    }

    ## cleanup
    unlink(c('./files/images/',vds.id,'/',plots))

    ## should also md5 check the dumped images?

})

rcouchutils::couch.deletedb(parts)
