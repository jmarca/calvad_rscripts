source('../../lib/load.pems.raw.file.R',chdir=TRUE)

library(testthat)

test_that("recoding lanes does what it should", {

    raw.data <- c('n','o')
    lanes <- 1
    y <- vds.lane.numbers(lanes,raw.data)
    expect_that(y,equals(c("nr1", "or1")))

    lanes <- 2
    y <- vds.lane.numbers(lanes,raw.data)
    expect_that(y,equals(c("nl1", "ol1","nr1", "or1")))

    lanes <- 3
    y <- vds.lane.numbers(lanes,raw.data)
    expect_that(y,equals(c("nl1", "ol1","nr2", "or2","nr1", "or1")))

})
