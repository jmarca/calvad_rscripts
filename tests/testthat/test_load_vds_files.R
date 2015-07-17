config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
year <- 2012
path <- 'files'
fname <- "809126_ML_2012"


test_that(
    'can load a so-called hidden rdata RData file',
    {
        df.vds <- load.file(f='',
                            fname=fname,
                            year=year,
                            path=path)
        expect_is(df.vds,'data.frame')
        expect_that(dim(df.vds),equals(c(696965,7)))
        expect_that(sort(names(df.vds)),equals(
            c('nl1', 'nr1', 'nr2', 'ol1', 'or1', 'or2', 'ts')
            ))
    })
