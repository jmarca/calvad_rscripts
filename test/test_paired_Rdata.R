library('testthat')
source('../lib/paired.Rdata.R')

test_that("load.wim.pair.data() will return a big data frame", {

    year <- 2010
    ## fake entry, but real anyway
    ## 37 | 74135.4030687827 | south     |     4
    ## 87 | 23462.8692386871 | south     |     4

    ## fake pairing, but real anyway
    vds.id <- 1108541
    ## loaded this site in R cursor, got
    vds.nvars <- c("nl1", "nr3", "nr2", "nr1")
    lanes = length(vds.nvars)
    wim.ids <- data.frame(wim_id=c(37,87)
                         ,direction=c('S','S')
                         ,lanes=c(4,4)
                         ,distance=c(74135.4,23462.9))

    bigdata <- load.wim.pair.data(wim.ids,vds.nvars=vds.nvars,lanes=lanes,year=year)

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
