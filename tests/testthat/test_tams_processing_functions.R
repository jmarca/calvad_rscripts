config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('tams','lanes')
result <- rcouchutils::couch.makedb(parts)
tams.site <- 7005
year <- 2017
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'files'


## clean up any earlier tests output
tams.data.path <- paste(tams.path,year,tams.site,sep='/')

drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
if(length(drop_rdatas)>0) {
    unlink(drop_rdatas)
}
drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
if(length(drop_pngs)>0) {
    unlink(drop_pngs)
}




test_that("parts of processing CSV data work okay", {

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
    testthat::expect_equal(sort(directions),c('E','W'))

    tams.data.E <- tams.data[['E']]
    tams.data.W <- tams.data[['W']]

    testthat::expect_is(tams.data.E,'data.frame')
    testthat::expect_is(tams.data.W,'data.frame')

    testthat::expect_that(dim(tams.data.E),testthat::equals(c(1494,10)))
    testthat::expect_that(dim(tams.data.W),testthat::equals(c(1494,10)))

    testthat::expect_equal(sort(names(tams.data.E)),
                           c("day",
                             "heavyheavy_r1",
                             "heavyheavy_r2",
                             "hr",
                             "n_r1",
                             "n_r2",
                             "not_heavyheavy_r1",
                             "not_heavyheavy_r2",
                             "tod",
                             "ts"))

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
        testthat::expect_equal(dim(load.df),c(1494,10))
        testthat::expect_equal(load.df, tams.data[[dir]])
    }


    load.from.fs <- calvadrscripts::load.tams.from.fs(tams.site,year,tams.path,parts)
    ## didn't yet save lane info, expect nothing back
    testthat::expect_equal(length(load.from.fs),1)
    testthat::expect_equal(load.from.fs,'todo')

    ## save to couchdb, then do it again
    for(direction in names(tams.data)){
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        is.stored <- rcouchutils::couch.check.state(year=year,
                                                    id=cdb.tamsid,
                                                    state='lanes',
                                                    db=parts)
        testthat::expect_equal(is.stored,'todo')
    }
    for(direction in names(tams.data)){
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        rcouchutils::couch.set.state(year=year,
                                     id=cdb.tamsid,
                                     doc=list('lanes'=site.lanes),
                                     db=parts)
    }
    for(direction in names(tams.data)){
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        is.stored <- rcouchutils::couch.check.state(year=year,
                                                    id=cdb.tamsid,
                                                    state='lanes',
                                                    db=parts)
        testthat::expect_equal(is.stored,site.lanes)
    }
    load.from.fs <- calvadrscripts::load.tams.from.fs(tams.site,year,tams.path,parts)
    ## didn't yet save lane info, expect nothing back
    testthat::expect_equal(length(load.from.fs),2)
    ## test the data
    testthat::expect_equal(load.from.fs[[1]],tams.data)
    testthat::expect_equal(sort(names(load.from.fs[[1]])),c('E','W'))
    ## test the site.lanes
    testthat::expect_equal(load.from.fs[[2]],site.lanes)



    drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_rdatas)>0) {
        unlink(drop_rdatas)
    }
    drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_pngs)>0) {
        unlink(drop_pngs)
    }

})

result <- rcouchutils::couch.deletedb(parts)
