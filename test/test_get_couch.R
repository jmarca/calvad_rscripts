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
