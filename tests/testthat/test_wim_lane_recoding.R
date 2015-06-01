config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('wim','lanes ')
result <- rcouchutils::couch.makedb(parts)
path <-  './files'

test_that("can recode lanes for plotting",{
    site_no <- 37
    direction <- 'S'
    year <- 2012

    my_df <- get.amelia.wim.file.local(site_no=site_no,
                                       direction=direction,
                                       path=path,
                                       year=year)
    ## set up a reconfigured dataframe
    df.merged <- wim.medianed.aggregate.df(my_df)
    recoded <- recode.df.wim( df.merged )

    expect_that(levels(recoded$lane),equals(c('left','lane 2','lane 3','right')))
    expect_that(table(recoded$lane)[['left']],equals(0))
    for(l in c('lane 2','lane 3','right')){
        expect_that(table(recoded$lane)[[l]],equals(8784))
    }
})

rcouchutils::couch.deletedb(parts)
