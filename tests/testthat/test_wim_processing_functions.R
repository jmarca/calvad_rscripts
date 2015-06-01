config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('wim','lanes ')
result <- rcouchutils::couch.makedb(parts)
path <-  './files'

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,port=config$postgresql$port
                  ,host=config$postgresql$host
                  ,dbname='spatialvds') ## need the live db for this test

test_that("oldway and new way are the same", {

    wim_site_no <- 108
    year <- 2012
    wim.path <- 'files/wim/'

    directions <- get.wim.directions(wim_site_no,con=con)
    expect_that(directions$direction[1],equals('N'))
    expect_that(directions$direction[2],equals('S'))


})
