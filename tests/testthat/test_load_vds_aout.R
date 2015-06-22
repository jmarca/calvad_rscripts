config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('verify','db','dump','trials')
rcouchutils::couch.makedb(parts)

## setup pg connection
library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)

test_that(
    "verify db dump works okay",{
        vds.id <- 317676
        file  <- './files/317676_ML_2012.120.imputed.RData'
        fname <- '317676_ML_2012'
        path <- './files'
        year <- 2012

        target.file <- verify.db.dump(fname=fname,
                                      path=path,
                                      year=year,
                                      con=con)
        expect_that(target.file,equals(make.db.dump.output.file(path,vds.id,year)))
        finfo <- file.info(target.file)
        print(finfo)

})


rcouchutils::couch.deletedb(parts)
