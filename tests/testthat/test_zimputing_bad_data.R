config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('vds','impute','baddata')
rcouchutils::couch.makedb(parts)

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)


test_that("complaining about error conditions works okay",{

    file  <- './files/716126_ML_2012.df.2012.RData'
    fname <- '716126_ML_2012'
    vds.id <- 716126
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
                                                con=con,
                                                trackingdb=parts)

    print(result)
    expect_that(result,equals("2 message All of the imputations resulted in a covariance matrix that is not invertible."))


    saved.state.doc <- rcouchutils::couch.get(db=parts,docname=vds.id)

    ## print(saved.state.doc)
    ## print(saved.state.doc[[year]])
    state <- saved.state.doc[[paste(year)]]
    expect_that(sort(names(state)),
                equals(c('raw_imputation_code',
                         'raw_imputation_message',
                         'vdsraw_chain_lengths',
                         'vdsraw_max_iterations')))
    expect_that(state$raw_imputation_code,equals(2))
    expect_that(state$raw_imputation_message,equals("All of the imputations resulted in a covariance matrix that is not invertible."))

})


test_that('sending a failed imputation to get.and.plot.vds.amelia does not crash',{

    file  <- './files/716126_ML_2012.df.2012.RData'
    fname <- '716126_ML_2012'
    vds.id <- 716126
    year <- 2012
    seconds <- 120
    path <- '.'

    context("plotting broken amelia result")
    df_agg <- get.and.plot.vds.amelia(
        pair=vds.id,
        year=year,
        doplots=TRUE,
        remote=FALSE,
        path=path,
        force.plot=TRUE,
        trackingdb=parts)

    expect_that(df_agg,is_null())

    context('plot raw data')
    result <- plot_raw.data(fname,file,path,year,vds.id
                           ,remote=FALSE
                           ,force.plot=TRUE
                           ,trackingdb=parts)

    doc <- rcouchutils::couch.get(parts,vds.id)
    attachments <- doc[['_attachments']]
    expect_that(attachments,is_a('list'))
    ## print(sort(names(attachments)))
    expect_that(sort(names(attachments)),equals(
        c(paste(vds.id,year,"raw_001.png",sep='_')
        , paste(vds.id,year,"raw_002.png",sep='_')
        , paste(vds.id,year,"raw_003.png",sep='_')
        , paste(vds.id,year,"raw_004.png",sep='_'))))

    ## should also md5 check the dumped images?


})

unlink('./files/716126_ML_2012.120.imputed.RData')

rcouchutils::couch.deletedb(parts)
