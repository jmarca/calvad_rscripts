source('../../lib/get.medianed.amelia.vds.R',chdir=TRUE)
library(testthat)

test_that("plotting-related code works okay",{

    file <- './files/718204_ML_2012.120.imputed.RData'
    res <- load(file)
    expect_that(res,equals('df.vds.agg.imputed'))

    df.merged <- condense.amelia.output(df.vds.agg.imputed)
    expect_that(dim(df.merged),equals(c(8784,12)))

    varnames <- names(df.merged)
    expect_that(varnames,equals(c("ts", "nl1","nr3","nr2","nr1",
                                  "ol1", "or3","or2","or1",
                                  "obs_count","tod","day")))

    expect_that(min(df.merged$nl1),equals(0.0))
    expect_that(median(df.merged$nl1),equals(1331.2886693577))
    expect_that(mean(df.merged$nl1),equals(1224.8003974521))
    expect_that(max(df.merged$nl1),equals(2362.7500629902))

### that should be good enough to verify that condense hasn't changed
### its spots

    twerked.df <- recode.df.vds(df.merged)

    expect_that(twerked.df,equals(c(35136,7)))
    expect_that(names(twerked.df),equals(c("ts","tod","day",
                                          "obs_count","lane","volume",
                                          "occupancy")))


})
