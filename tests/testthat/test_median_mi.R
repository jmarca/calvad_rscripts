source('../../lib/get.medianed.amelia.vds.R',chdir=TRUE)

library(testthat)

test_that("oldway and new way are the same", {

    res <- load("./files/718204_ML_2012.120.imputed.RData")
    expect_that(res,equals('df.vds.agg.imputed'))

    df_agg_old <- NULL
    df_agg <- NULL

    df.c <- NULL
    for(i in 1:length(df.vds.agg.imputed$imputations)){
        df.c <- rbind(df.c,df.vds.agg.imputed$imputations[[i]])
    }

    oldway.timer <- system.time(
        df_agg_old <- medianed.aggregate.df.oldway(df.c)
        df_agg_old <- unzoo.incantation(df_agg_old)
        )

    newway.timer <- system.time(
        df_agg <- medianed.aggregate.df(df.c)
        )

    expect_that(df_agg$nl1,equals(df_agg_old$nl1))
    expect_that(df_agg$nr3,equals(df_agg_old$nr3))
    expect_that(df_agg$nr2,equals(df_agg_old$nr2))
    expect_that(df_agg$nr1,equals(df_agg_old$nr1))

    expect_that(df_agg$ol1,equals(df_agg_old$ol1))
    expect_that(df_agg$or3,equals(df_agg_old$or3))
    expect_that(df_agg$or2,equals(df_agg_old$or2))
    expect_that(df_agg$or1,equals(df_agg_old$or1))

    expect_that(df_agg$ts,equals(df_agg_old$ts))

    expect_that(df_agg$tod,equals(df_agg_old$tod))

    expect_that(df_agg$day,equals(df_agg_old$day))


})
