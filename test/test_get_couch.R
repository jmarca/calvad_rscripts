library('testthat')
source('../lib/get_couch.R',chdir=TRUE)

test_that("get.wim.vds.pairs() will get all pairs for a given year", {

    year <- 2010
    w.v.p <- get.vds.wim.pairs(year)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(87,5)))
    ## was going to check year == 2010, but why bother

    w.v.p <- get.vds.wim.pairs(2014)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(0,0)))

    w.v.p <- get.vds.wim.pairs(2007)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(78,5)))

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
    result <- couch.get.attachment('vdsdata%2ftracking'
                                   ,vdsid
                                   ,paired.attachment
                                   ,local=FALSE)
    expect_that(is.null(df.merged),is_false())
    expect_that(is.data.frame(df.merged),is_true())
    expect_that(dim(df.merged),equals(c(8755,37)))

})
