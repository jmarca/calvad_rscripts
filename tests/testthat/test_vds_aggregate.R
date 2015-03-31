source('../../lib/vds.aggregate.R',chdir=TRUE)
library(testthat)

## test_that("oldway and new way are the same",{

##     file <- './tests/testthat/files/1211682_ML_2012.df.2012.RData'
##     res <- load(file)
##     expect_that(res,equals('df'))

##     ts <- df$ts
##     df$ts <- NULL


##     df_agg_old <- NULL
##     df_agg <- NULL

##     ## oldway.timer <- system.time(
##     ##     df_agg_old <- oldway.vds.aggregate(df,ts,seconds=120)
##     ##     )
##     ## print(oldway.timer)

##     newway.timer <- system.time(
##         df_agg <- vds.aggregate(df,ts,seconds=120)
##         )

##     print(newway.timer)
##     expect_that(df_agg$ts,equals(df_agg_old$ts))

##     expect_that(dim(df_agg)[1],equals(dim(df_agg_old)[1]))

##     expect_that(df_agg$nl1,equals(df_agg_old$nl1))
##     expect_that(df_agg$nr1,equals(df_agg_old$nr1))

##     expect_that(df_agg$ol1,equals(df_agg_old$ol1))
##     expect_that(df_agg$or1,equals(df_agg_old$or1))

##     expect_that(df_agg$tod,equals(df_agg_old$tod))

##     expect_that(df_agg$day,equals(df_agg_old$day))

## ##########
## ######    District 7 file
## #########
##     file <- './tests/testthat/files/718204_ML_2012.df.2012.RData'

##     res <- load(file)
##     expect_that(res,equals('df'))

##     ts <- df$ts
##     df$ts <- NULL


##     df_agg_old <- NULL
##     df_agg <- NULL

##     oldway.timer <- system.time(
##         df_agg_old <- oldway.vds.aggregate(df,ts,seconds=120)
##         )
##     print(oldway.timer)

##     newway.timer <- system.time(
##         df_agg <- vds.aggregate(df,ts,seconds=120)
##         )
##     print(newway.timer)

##     expect_that(df_agg$ts,equals(df_agg_old$ts))

##     expect_that(dim(df_agg)[1],equals(dim(df_agg_old)[1]))

##     expect_that(df_agg$nl1,equals(df_agg_old$nl1))
##     expect_that(df_agg$nr1,equals(df_agg_old$nr1))
##     expect_that(df_agg$nr2,equals(df_agg_old$nr2))
##     expect_that(df_agg$nr3,equals(df_agg_old$nr3))

##     expect_that(df_agg$ol1,equals(df_agg_old$ol1))
##     expect_that(df_agg$or1,equals(df_agg_old$or1))
##     expect_that(df_agg$or2,equals(df_agg_old$or2))
##     expect_that(df_agg$or3,equals(df_agg_old$or3))

##     expect_that(df_agg$tod,equals(df_agg_old$tod))

##     expect_that(df_agg$day,equals(df_agg_old$day))


## })

## This test passed and now I don't run it anymore'
## keep this around only because I need a different test
