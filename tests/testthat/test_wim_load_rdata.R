expect_that('can load rdata from a file for WIM',{
    df <- get.wim.rdata(wim.site = 101,
                        year = 2012,
                        direction = 'N',
                        wim.path='./files')
    expect_that(df,is_a('data.frame'))
    ## blah blah
})
