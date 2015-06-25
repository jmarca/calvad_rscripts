config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('vds','plots','problems')
result <- rcouchutils::couch.makedb(parts)

path <- './files'
mysavepath <- './files/images'
if(!file.exists(mysavepath)){dir.create(mysavepath)}

context('some problem data.frames')
test_that("plotting imputed data code works okay",{

    file <- './files/318401_ML_2012.df.2012.RData'
    fname <- '318401_ML_2012'
    vds.id <- 318401
    year <- 2012

    env <- new.env()
    res <- load(file=file,envir=env)
    expect_that(res,equals('df'))
    df.raw <- env[[res]]

    ts <- df.raw$ts
    df.raw$ts <- NULL
    ## print(summary(df.raw))
    ## aggregate up to an hour?
    df.merged <- vds.aggregate(df.raw,ts,seconds=3600)
    ## print(summary(df.merged))

    na_filter <- is.na(df.merged$nl1)
    not_enough <- df.merged$obs_count == 3600/30

    ## for this case, expect that everything is NA because there are
    ## no hours with perfect data

    expect_that(na_filter,equals(!not_enough))
    expect_that(length(df.merged$nl1[na_filter]),equals(length(df.merged$nl1)))

    df.merged <- vds.aggregate(df.raw,ts,seconds=120)

    na_filter <- is.na(df.merged$nl1)
    not_enough <- df.merged$obs_count ==  120/30
    expect_that(na_filter,equals(! not_enough))
    expect_true(length(na_filter[na_filter])<length(df.merged$nl1))

    ## should plot just fine when calling plot_raw.data
    context('plot raw data')
    result <- plot_raw.data(fname,file,path,year,vds.id
                           ,remote=FALSE
                           ,force.plot=TRUE
                           ,trackingdb=parts)

    doc <- rcouchutils::couch.get(parts,vds.id)
    attachments <- doc[['_attachments']]
    expect_that(attachments,is_a('list'))
    expect_that(sort(names(attachments)),equals(
        c(paste(vds.id,year,"raw_001.png",sep='_')
        , paste(vds.id,year,"raw_002.png",sep='_')
        , paste(vds.id,year,"raw_003.png",sep='_')
        , paste(vds.id,year,"raw_004.png",sep='_'))))

})


rcouchutils::couch.deletedb(parts)
