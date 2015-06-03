config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('extract','unique','lanes')
result <- rcouchutils::couch.makedb(parts)
year <- 2012
path <- './files'

test_that("can extract unique lanes from wim data",{
    site_no <- 37
    direction <- 'S'

    my_df <- get.wim.rdata(wim.site=site_no,
                           direction=direction,
                           wim.path=path,
                           year=year)

    lanes <- extract_unique_lanes(df=my_df)
    expect_that(sort(lanes),equals(c('r1','r2','r3')))

    ## can also call with varnames as argument
    varnames <- grep(pattern='r3$',
                     perl=TRUE,
                     invert=TRUE,
                     value=TRUE,
                     x=names(my_df))

    lanes <- extract_unique_lanes(varnames=varnames)
    expect_that(sort(lanes),equals(c('r1','r2')))


})

test_that("can extract unique lanes from vds data",{
    vds.id <- 737237

    my_df <- calvadrscripts::get.and.plot.vds.amelia(
        pair=vds.id,
        year=year,
        doplots=FALSE,
        remote=FALSE,
        path=path,
        force.plot=FALSE,
        trackingdb=parts)

    lanes <- extract_unique_lanes(df=my_df)
    expect_that(sort(lanes),equals(c('l1','r1','r2')))

})

test_that("passing a data.frame as unnamed first arg isn't fatal",{
    site_no <- 37
    direction <- 'S'

    my_df <- get.wim.rdata(wim.site=site_no,
                           direction=direction,
                           wim.path=path,
                           year=year)
    lanes <- extract_unique_lanes(my_df)
    expect_that(sort(lanes),equals(c('r1','r2','r3')))

})

result <- rcouchutils::couch.deletedb(parts)
