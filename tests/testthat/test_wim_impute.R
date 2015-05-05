config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','impute','tests')
rcouchutils::couch.makedb(parts)

library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally

con <-  dbConnect(m
                  ,user=config$postgresql$auth$username
                  ,host=config$postgresql$host
                  ,dbname=config$postgresql$db)


wim.site <-  108
wim.dirs <- c('N','S')
wim.path <- './data'
seconds <- 3600
year <- 2012

test_that('load wim from db works okay',{
    df.wim <- load.wim.data.straight(wim.site=wim.site,year=year,con=con)
    expect_that(dim(df.wim),equals(c(2190512,15)))
    df.wim.split <- split(df.wim, df.wim$direction)
    directions <- names(df.wim.split)
    expect_that(sort(directions),equals(wim.dirs))
    df.wim.speed <- get.wim.speed.from.sql(wim.site=wim.site,year=year,con=con)
    expect_that(dim(df.wim.speed),equals(c(214259,5)))
    df.wim.speed.split <- split(df.wim.speed, df.wim.speed$direction)
    direction <- 'S'
    df.wim.d <- process.wim.2(df.wim.split[[direction]])
    df.wim.s <- df.wim.speed.split[[direction]]
    df.wim.d <- wim.additional.variables(df.wim.d)
    df.wim.dagg <- wim.lane.and.time.aggregation(df.wim.d)

})

test_that("wim impute works okay",{
    df <- get.wim.rdata(wim.site = 108,
                        year = 2012,
                        direction = 'S',
                        wim.path='./data')
    expect_that(df,is_a('data.frame'))
    df.amelia <- fill.wim.gaps(df.wim=df,
                               plotfile = 'images/test.wim.amelia.plot.png')
    expect_that(df.amelia,is_a('amelia'))

})
