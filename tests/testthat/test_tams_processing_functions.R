config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('tams','lanes ')
result <- rcouchutils::couch.makedb(parts)
tams.site <- 7005
year <- 2016
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'tests/testthat'
direction <- 'E'


library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,port=config$postgresql$port
                  ,host=config$postgresql$host
                  ,dbname='spatialvds') ## need the live db for this test

test_that("oldway and new way are the same", {

    tams_site_no <- 108
    year <- 2012
    tams.path <- 'files/tams/'

    directions <- get.tams.directions(tams_site_no,con=con)
    expect_that(directions$direction[1],equals('N'))
    expect_that(directions$direction[2],equals('S'))


})
