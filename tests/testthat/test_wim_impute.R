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
    cdb.wimid <- paste('wim',wim.site,direction,sep='.')
    df.wim.d <- process.wim.2(df.wim.split[[direction]])
    df.wim.s <- df.wim.speed.split[[direction]]
    df.wim.d <- wim.additional.variables(df.wim.d)
    df.wim.dagg <- wim.lane.and.time.aggregation(df.wim.d)
    expect_that(dim(df.dagg),equals(c(8784,21)))
    df.wim.sagg <- make.speed.aggregates(df.wim.s)
    expect_that(dim(df.sagg),equals(c(8784,5)))

    df.wim.d.joint <- merge(df.wim.dagg,df.wim.sagg,all=TRUE)
    expect_that(dim(df.wim.d.joint),equals(c(8784,25)))

    df.wim.d.joint <- add.time.of.day(df.wim.d.joint)

    attach.files <- plot_wim.data(df.wim.d.joint,wim.site,direction,year
                              ,fileprefix='raw'
                              ,subhead='\npre imputation'
                              ,force.plot=TRUE
                              ,trackingdb=parts)
    expect_that(attach.files,equals(c(
        "images/108/S/108_S_2012_raw_001.png",
        "images/108/S/108_S_2012_raw_002.png",
        "images/108/S/108_S_2012_raw_003.png",
        "images/108/S/108_S_2012_raw_004.png",
        "images/108/S/108_S_2012_raw_005.png",
        "images/108/S/108_S_2012_raw_006.png"
        )))
    for(f2a in c(attach.files)){
        rcouchutils::couch.attach(parts,cdb.wimid,f2a)
    }
    ## need to check that saved okay with this second call
    attach.files <- plot_wim.data(df.wim.d.joint,wim.site,direction,year
                              ,fileprefix='raw'
                              ,subhead='\npre imputation'
                              ,force.plot=FALSE
                                 ,trackingdb=parts)

    ## it should have bailed out
    expect_that(attach.files,equals(1))

    context('Amelia call')
    df.wim.amelia <- fill.wim.gaps(df.wim=df.wim.d.joint)
    expect_that(df.wim.amelia$code,equals(1))

    context('post process amelia result')
    df.wim.agg.amelia <- wim.medianed.aggregate.df(df.wim.amelia)
    expect_that(dim(df.wim.agg.amelia),equals(c(8784,27)))

    attach.files <- plot_wim.data(df.wim.agg.amelia,wim.site,direction,year
                              ,fileprefix='imputed'
                              ,subhead='\npost imputation'
                              ,force.plot=TRUE
                              ,trackingdb=parts)
    expect_that(attach.files,equals(c(
        "images/108/S/108_S_2012_imputed_001.png",
        "images/108/S/108_S_2012_imputed_002.png",
        "images/108/S/108_S_2012_imputed_003.png",
        "images/108/S/108_S_2012_imputed_004.png",
        "images/108/S/108_S_2012_imputed_005.png",
        "images/108/S/108_S_2012_imputed_006.png"
        )))
    for(f2a in c(attach.files)){
        rcouchutils::couch.attach(parts,cdb.wimid,f2a)
    }
    attach.files <- plot_wim.data(df.wim.agg.amelia,wim.site,direction,year
                              ,fileprefix='imputed'
                              ,subhead='\npost imputation'
                              ,force.plot=FALSE
                              ,trackingdb=parts)

    ## it should have bailed out
    expect_that(attach.files,equals(1))

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
