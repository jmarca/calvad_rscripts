config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally
con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)

test_that("dump code works okay",{

    file <- './files/718204_ML_2012.120.imputed.RData'

    res <- load(file)
    expect_that(res,equals('df.vds.agg.imputed'))

    tmd <- tempdir()

    verify.db.dump('718204_ML_2012.120.imputed.RData',
                   path=tmd,
                   2012,120,df.vds.agg.imputed=df.vds.agg.imputed,
                   con
                   )
    vds.id <- 718204
    impsagg <- impute.aggregate(df.vds.agg.imputed)

    db.ready.dump(imps = impsagg,
                  vds.id = vds.id,
                  path=tmd,
                  year=2012,
                  con=con  )

    datfile <- dir(path=tmd,pattern='vds_hour_agg',
                   full.names=TRUE,recursive=TRUE)
    expect_that(datfile[1],matches(paste('vds_hour_agg',vds.id,sep='.')))

})
