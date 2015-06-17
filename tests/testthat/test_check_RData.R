config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('check','amelia','files')
result <- rcouchutils::couch.makedb(parts)
year <- 2012
path <- './files'
goodfile  <- paste(path,'1108541_ML_2012.120.imputed.RData',sep='/')
notamelia <- paste(path,'1211682_ML_2012.df.2012.RData',sep='/')
badamelia <- paste(path,'801320_ML_2012.120.imputed.RData',sep='/')
wimamelia <- paste(path,'2012/87/S/wim87S.3600.imputed.RData',sep='/')

test_that('decode amelia output works',{
    result <- decode_amelia_output_file(wimamelia)
    expect_that(result[1,'year'],equals(2012))
    expect_that(result[1,'direction'],equals('S'))
    expect_that(result[1,'site_no'],equals(87))

    result <- decode_amelia_output_file(goodfile)
    expect_that(result[1,'year'],equals(2012))
    expect_that(result[1,'vds_id'],equals(1108541))
})

test_that('check amelia works',{
    result <- amelia_output_file_status(goodfile)
    expect_that(result,equals(0))

    result <- amelia_output_file_status(notamelia)
    expect_that(result,equals(2))

    result <- amelia_output_file_status(badamelia)
    expect_that(result,equals(2))

    result <- amelia_output_file_status(wimamelia)
    expect_that(result,equals(0))

})

result <- rcouchutils::couch.deletedb(parts)
