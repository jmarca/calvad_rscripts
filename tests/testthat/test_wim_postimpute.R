source('../../lib/process.wim.site.R',chdir=TRUE)

library(testthat)

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user=psqlenv[2]
                  ,host=psqlenv[1]
                  ,dbname="spatialvds")

trackingdb <- paste('test',trackingdb,sep='_')
couch.makedb(trackingdb,local=TRUE)

test_that("oldway and new way are the same", {

    res <- load("./data/2010/108/N/wim108N.3600.imputed.RData")
    expect_that(res,equals('df.wim.amelia'))

    post.impute.plots(108,2010,wim.path='./data/')

    df_agg_old <- NULL
    df_agg <- NULL



})
