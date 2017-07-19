config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('tams','lanes ')
result <- rcouchutils::couch.makedb(parts)
tams.site <- 7005
year <- 2017
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'tests/testthat/files'




test_that("oldway and new way are the same", {

    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'E',tams.path)
    testthat::expect_equal(load.df,'todo')
    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'W',tams.path)
    testthat::expect_equal(load.df,'todo')

    tams.data <- calvadrscripts::load.tams.from.csv(tams.site=tams.site,
                                    year=year,
                                    tams.path=tams.path)
    tams.data <- calvadrscripts::reshape.tams.from.csv(tams.csv=tams.data,
                                       year=year,
                                       tams.path = tams.path)

    testthat::expect_is(tams.data,'list')

    site.lanes <- tams.data[[2]]
    testthat::expect_equal(site.lanes,5)

    tams.data <- tams.data[[1]]
    testthat::expect_is(tams.data,'list')


    directions <- names(tams.data)
    testthat::expect_that(sort(directions),testthat::equals(c('E','W')))

    tams.data.E <- tams.data[['E']]
    tams.data.W <- tams.data[['W']]

    testthat::expect_is(tams.data.E,'data.frame')
    testthat::expect_is(tams.data.W,'data.frame')

    testthat::expect_that(dim(tams.data.E),testthat::equals(c(2131,10)))
    testthat::expect_that(dim(tams.data.W),testthat::equals(c(2131,10)))

    testthat::expect_that(sort(names(tams.data.E)),
                          testthat::equals(
                                        c("day",
                                          "heavyheavy_r1",
                                          "heavyheavy_r2",
                                         "hr",
                                         "n_r1",
                                         "n_r2",
                                         "not_heavyheavy_r1",
                                         "not_heavyheavy_r2",
                                         "tod",
                                         "ts")))

    testthat::expect_equal(sort(names(tams.data.E)),
                          sort(names(tams.data.W)))

    ## saved files for later use

    isa.RData <- dir(paste(tams.path,year,tams.site,sep='/'), pattern='RData$',full.names=FALSE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
    testthat::expect_equal(sort(isa.RData),c( "E/7005.E.2017.tams.agg.RData", "W/7005.W.2017.tams.agg.RData"))

    ## those files equal what I have now, less knowledge of total
    ## number of lanes, so they are a little bit useless I guess

    ## but let's check them anyway

    for (dir in names(tams.data)){
        load.df <- calvadrscripts::load.tams.from.file(tams.site,year,dir,tams.path)
        testthat::expect_is(load.df,'data.frame')
        testthat::expect_equal(dim(load.df),c(2131,10))
        testthat::expect_equal(load.df, tams.data[[dir]])
    }

})
