config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

test_that("get.wim.vds.pairs() will get all pairs for a given year", {

    year <- 2010
    w.v.p <- get.vds.wim.pairs(year)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(86,5)))
    ## was going to check year == 2010, but why bother

    w.v.p <- get.vds.wim.pairs(2014)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(0,0)))

    w.v.p <- get.vds.wim.pairs(2007)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(78,5)))

})

test_that("get.vds.paired.to.wim() works okay", {

    year <- 2012
    w.v.p <- get.vds.paired.to.wim(year,site_no=66,direction='S')
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(w.v.p$year,equals(2012))
    expect_that(w.v.p$cdb.wimid,equals('wim.66.S'))
    expect_that(w.v.p$doc,equals('wim.66.S.vdsid.821071.2012.paired.RData'))

})

test_that("get.Rdata.view works okay",{

    vdsid <- 1114696
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    expect_that(paired.attachment,equals("wim.37.S.vdsid.1114696.2010.paired.RData"))

    vdsid <- 1114694
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    expect_that(paired.attachment,is_equivalent_to(list()))

})


## this is really another bit of library, but so what
## high level function is used here
test_that("can get attached RData file okay",{

    vdsid <- 1114696
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    result <- rcouchutils::couch.get.attachment('vdsdata%2ftracking'
                                               ,vdsid
                                               ,paired.attachment
                                                )
    expect_that(result,is_a('list'))

    varnames <- names(result)[1]
    expect_that(varnames,equals('df.merged'))

    barfl <- result[[1]][[varnames]]

    expect_that(is.data.frame(barfl),is_true())
    expect_that(dim(barfl),equals(c(8755,37)))

})
