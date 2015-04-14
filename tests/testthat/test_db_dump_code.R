source('./lib/vds.processing.functions.R',chdir=TRUE)


library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user=psqlenv[2]
                  ,host='192.168.0.1'
                  ,dbname="spatialvds")

library(testthat)


test_that("dump code works okay",{

    file <- './files/718204_ML_2012.120.imputed.RData'

    res <- load(file)
    expect_that(res,equals('df.vds.agg.imputed'))

    tmd <- tempdir()

    verify.db.dump('718204_ML_2012.120.imputed.RData',
                   path=tmd,
                   2012,120,df.vds.agg.imputed=df.vds.agg.imputed,
                   con
                   )

    impsagg <- impute.aggregate(df.vds.agg.imputed)

    db.ready.dump(impsagg,  718204, tmd, con  )

})
