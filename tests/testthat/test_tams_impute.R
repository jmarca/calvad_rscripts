config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('tams','lanes ')
result <- rcouchutils::couch.makedb(parts)
tams.site <- 7005
year <- 2017
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'tests/testthat/files'




test_that("oldway and new way are the same", {

    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'E',tams.path)
    testthat::expect_equal(load.df,'todo')
    load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'W',tams.path)
    testthat::expect_equal(load.df,'todo')

    tams.data <- calvadrscripts::load.tams.from.csv(tams.site=tams.site,
                                    year=year,
                                    tams.path=tams.path)
    tams.data <- calvadrscripts::reshape.tams.from.csv(tams.csv=tams.data,
                                       year=year,
                                       tams.path = tams.path)

    testthat::expect_is(tams.data,'list')

    site.lanes <- tams.data[[2]]
    testthat::expect_equal(site.lanes,5)

    tams.data <- tams.data[[1]]
    testthat::expect_is(tams.data,'list')


    directions <- names(tams.data)
    testthat::expect_that(sort(directions),testthat::equals(c('E','W')))

    direction <- 'E'
    cdb.tamsid <- paste('tams',tams.site,direction,sep='.')

    attach.files <- plot_tams.data(tams.data[[direction]]
                                  ,tams.site
                                  ,direction
                                  ,year
                                  ,lanes.count
                                  ,fileprefix='raw'
                                  ,subhead='\npre imputation'
                                  ,force.plot=TRUE
                                  ,trackingdb=parts
                                  ,tams.path=tams.path)


    testthat::expect_that(attach.files,equals(
        paste("images/",tams.site,"/",direction,"/",
              tams.site,"_",direction,"_",year,"_raw_",
              c("001.png",
                "002.png",
                "003.png",
                "004.png",
                "005.png",
                "006.png"),
              sep='')
        )))
    for(f2a in c(attach.files)){
        rcouchutils::couch.attach(parts,cdb.tamsid,f2a)
    }
    ## need to check that saved okay with this second call
    attach.files <- plot_tams.data(tams.data[[direction]]
                                  ,tams.site
                                  ,direction
                                  ,year
                                  ,lanes.count
                                  ,fileprefix='raw'
                                  ,subhead='\npre imputation'
                                  ,force.plot=FALSE
                                  ,trackingdb=parts
                                  ,tams.path=tams.path)

    ## it should have bailed out
    testthat::expect_that(attach.files,equals(1))

    context('Amelia call')
    df.tams.amelia <- fill.tams.gaps(df.tams=tams.data[[direction]])
    testthat::expect_equal(df.tams.amelia$code,1)

    context('post process amelia result')
    df.tams.agg.amelia <- tams.medianed.aggregate.df(df.tams.amelia)
    testthat::expect_that(dim(df.tams.agg.amelia),equals(c(8784,27)))

    attach.files <- plot_tams.data(df.tams.agg.amelia,tams.site,direction,year
                              ,fileprefix='imputed'
                              ,subhead='\npost imputation'
                              ,force.plot=TRUE
                              ,trackingdb=parts)
    testthat::expect_equal(attach.files,
                           paste("images/",tams.site,"/",direction,"/",
                                 tams.site,"_",direction,"_2012_imputed_",
                                 c("001.png",
                                   "002.png",
                                   "003.png",
                                   "004.png",
                                   "005.png",
                                   "006.png"),
                                 sep='')
                           )
    for(f2a in c(attach.files)){
        rcouchutils::couch.attach(parts,cdb.tamsid,f2a)
    }
    attach.files <- plot_tams.data(df.tams.agg.amelia,tams.site,direction,year
                              ,fileprefix='imputed'
                              ,subhead='\npost imputation'
                              ,force.plot=FALSE
                              ,trackingdb=parts)

    ## it should have bailed out
    testthat::expect_equal(attach.files,1)

    ## clean up for next test
    rcouchutils::couch.delete(db=parts,docname=cdb.tamsid)
})

test_that("process tams  site also works okay",{
    list.df.tams.amelia <- process.tams.site(tams.site=tams.site,
                                           year=year,
                                           seconds=seconds,
                                           preplot=TRUE,
                                           postplot=TRUE,
                                           force.plot=FALSE,
                                           tams.path='./data',
                                           trackingdb=parts,
                                           con=con
                                           )
    testthat::expect_that(list.df.tams.amelia,is_a('list'))
    directions <- c('S','N')
    for(direction in directions){
        testthat::expect_that(list.df.tams.amelia[[direction]]$code,equals(1))
        docid <- paste('tams',tams.site,direction,sep='.')
        doc <- rcouchutils::couch.get(parts,docid)
        attachments <- doc[['_attachments']]
        testthat::expect_that(attachments,is_a('list'))
        ## print(sort(names(attachments)))
        testthat::expect_equal(sort(names(attachments)),
            c(paste(tams.site,direction,year,
                    c(rep('imputed',6),rep('raw',6)),
                    c("001.png",
                      "002.png",
                      "003.png",
                      "004.png",
                      "005.png",
                      "006.png"),
                    sep='_'))
            )
    }

})
rcouchutils::couch.deletedb(parts)
