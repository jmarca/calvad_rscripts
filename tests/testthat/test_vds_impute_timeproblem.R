config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('vds','impute','timingoff')
rcouchutils::couch.makedb(parts)

test_that("detect_broken_imputed_time works okay",{

    fname <- '317676_ML_2012'
    year <- 2012
    path <- './files'
    result <- detect_broken_imputed_time(fname=fname,
                                         year=year,
                                         path=path,
                                         delete_it=FALSE,
                                         trackingdb=parts)

    expect_true(result)

    fname <- '1108541_ML_2012'
    year <- 2012
    path <- './files'
    result <- detect_broken_imputed_time(fname=fname,
                                         year=year,
                                         path=path,
                                         delete_it=FALSE,
                                         trackingdb=parts)

    expect_false(result)

})

rcouchutils::couch.deletedb(parts)
