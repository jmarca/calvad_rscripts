## call this for calls inside impute routine
rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
get.config <- configr::configrr()
config <- get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))$postgresql

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$auth$username
                  ,host=config$host
                  ,dbname=config$db)

test_that("vds impute works okay",{

    file  <- './files/718204_ML_2012.df.2012.RData'
    fname <- '718204_ML_2012'
    vds.id <- 718204
    year <- 2012
    seconds <- 120
    path <- '.'
    result <- self.agg.impute.VDS.site.no.plots(fname=fname,
                                                f=file,
                                                path=path,
                                                year=year,
                                                seconds=seconds,
                                                goodfactor=3.5,
                                                maxiter=20,
                                                con=con)


    expect_that(result,equals(1))

})
