config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)

unlink('./files/1211682_ML_2012.120.imputed.RData')
test_that("vds impute works okay",{

    file  <- './files/1211682_ML_2012.df.2012.RData'
    fname <- '1211682_ML_2012'
    vds.id <- 1211682
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
    datfile <- dir(path='.',pattern='vds_hour_agg',full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste('vds_hour_agg',vds.id,sep='.')))

    datfile <- dir(path='.',
                   pattern=paste(vds.id,'.*imputed.RData$',sep=''),
                   full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste(vds.id,
                                         '_ML_',
                                         year,'.',
                                         seconds,'.',
                                         'imputed.RData',
                                         sep='')))

})

unlink('./vds_hour_agg.1211682.2012.dat')
