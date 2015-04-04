source('../../lib/process.wim.site.R',chdir=TRUE)

library(testthat)

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user='slash'
                  ,host='192.168.0.1'
                  ,dbname="spatialvds")

trackingdb <- paste('test',trackingdb,sep='_')
couch.makedb(trackingdb,local=TRUE)

test_that("oldway and new way are the same", {

    wim_site_no <- 108
    year <- 2012
    wim.path <- 'files/wim/'

    directions <- get.wim.directions(wim.site)
    expect_that(directions$direction[1],equals('N'))
    expect_that(directions$direction[2],equals('S'))


})
