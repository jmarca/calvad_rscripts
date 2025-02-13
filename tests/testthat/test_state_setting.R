config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('get','set','state')
result <- rcouchutils::couch.makedb(parts)

context('amelia chains')
test_that('store amelia chains output will work',{
    f <- './files/718204_ML_2012.120.imputed.RData'
    r <- load(f)
    vdsid <- 718204
    year <- 2012
    expect_that(r,equals('df.vds.agg.imputed'))
    r <- store.amelia.chains(df.amelia=df.vds.agg.imputed,
                             year=year,
                             detector.id=vdsid,
                             imputation.name='vdsraw',
                             maxiter=20,
                             db=parts
                             )
    expect_that(r,is_a('numeric'))
    expect_that(r,equals(0))
    saved.state <- rcouchutils::couch.check.state(year=year,
                                            id=vdsid,
                                            'vdsraw_chain_lengths',
                                                  db=parts)
    expect_that(saved.state,is_a('numeric'))
    expect_that(saved.state,
                equals(c(5,5,5,5,5)))

    saved.state <- rcouchutils::couch.check.state(year=year,
                                            id=vdsid,
                                            'vdsraw_max_iterations',
                                                  db=parts)
    expect_that(saved.state,is_a('numeric'))
    expect_that(saved.state,equals(0))


})

context('sanity check')
test_that('sanity check saves problems to couchdb statedb',{
    dbname <- rcouchutils::couch.makedbname(parts)
    f <- './files/1202248_ML_2010.df.2010.RData'
    r <- load(f)
    expect_that(r,equals('df'))
    ts <- df$ts
    df$ts <- NULL
    year <- 2010
    path <- '.'
    vdsid <- 1202248
    sc <- sanity.check(data=df,ts=ts,year=year,
                       vdsid=vdsid,
                       db=parts)
    expect_false(sc)
    saved.state <- rcouchutils::couch.check.state(year=year,
                                            id=vdsid,
                                            'rawdata',
                                            db=parts)
    expect_that(saved.state,is_a('character'))
    expect_that(saved.state,
                equals("mean volumes too low in lane: nr2 in raw vds file"))

    f <- './files/1213133_ML_2012.df.2012.RData'
    r <- load(f)
    expect_that(r,equals('df'))
    ts <- df$ts
    df$ts <- NULL
    year <- 2012
    path <- '.'
    vdsid <- 1213133
    sc <- sanity.check(data=df,ts=ts,year=year,
                       vdsid=vdsid,
                       db=parts)
    expect_false(sc)
    saved.state <- rcouchutils::couch.check.state(year=year,
                                            id=vdsid,
                                            'rawdata',
                                            db=parts)
    expect_that(saved.state,is_a('character'))
    expect_that(saved.state,equals('no rows of data in raw vds file'))

})

## test_that("can set and check state",{

##     dbname <- rcouchutils::couch.makedbname(parts)
##     doc <- list()
##     doc[['one potato']] <- 'two potatoes'
##     doc$beer <- 'food group'
##     doc$foo <- 123

##     id <- 'franged123'

##     set_result <- rcouchutils::couch.set.state(year='belsh',
##                                   id=id,
##                                   doc=doc,
##                                   db=dbname
##                                   )

##     expect_that(set_result,is_a('list'))
##     expect_that(names(set_result),equals(c('ok','id','rev')))
##     expect_that(set_result$ok,equals(TRUE))
##     expect_that(set_result$id,equals('franged123'))

##     state.check <- couch.check.state(year='belsh',
##                                      id=id,
##                                      state='beer',
##                                      db=dbname
##                                      )

##     expect_that(state.check,equals('food group'))

##     state.check <- couch.check.state(year='belsh',
##                                      id=id,
##                                      state='impute trucks',
##                                      db=dbname
##                                      )
##     expect_that(state.check,equals('todo'))

## })
rcouchutils::couch.deletedb(parts)
