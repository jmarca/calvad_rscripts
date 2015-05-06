config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

test_that('can load rdata from a file for WIM',{
    df <- get.wim.rdata(wim.site = 108,
                        year = 2012,
                        direction = 'S',
                        wim.path='./data')
    expect_that(df,is_a('data.frame'))
    df <- get.wim.rdata(wim.site = 37,
                        year = 2012,
                        direction = 'S',
                        wim.path='./files')
    expect_that(df,is_a('data.frame'))
    df <- get.wim.rdata(wim.site = 37,
                        year = 2012,
                        direction = 'S',
                        wim.path='./files'
                        ,filename.pattern = "imputed.RData$")
    expect_that(df,is_a('amelia'))
    ## blah blah
})
