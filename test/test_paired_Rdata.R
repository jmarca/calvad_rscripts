library('testthat')
source('../lib/paired.Rdata.R')

test_that("load.wim.pair.data() will return a big data frame", {

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
