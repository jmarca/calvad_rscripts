config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))


tams.site <- 7005
year <- 2017
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
## note that when developing this in the R interactive terminal, then
## yeah, this path should be set as tests/testthat/files.  But when
## running under the testing framework, it should be relative to
## tests/testthat, or just 'files'
## tams.path <- 'tests/testthat/files'
tams.path <- 'files'

tams.data.path <- paste(tams.path,year,tams.site,sep='/')
## make sure no artifacts from prior tests

## testthat::test_that("load data, plot raw, impute, and plot imputed all work", {

##     parts <- c('tams','impute')
##     result <- rcouchutils::couch.makedb(parts)
##     testthat::expect_equal(result$ok,TRUE)

##     drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
##     if(length(drop_rdatas)>0) {
##         unlink(drop_rdatas)
##     }
##     drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
##     if(length(drop_pngs)>0) {
##         unlink(drop_pngs)
##     }

##     load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'E',tams.path)
##     testthat::expect_equal(load.df,'todo')
##     load.df <- calvadrscripts::load.tams.from.file(tams.site,year,'W',tams.path)
##     testthat::expect_equal(load.df,'todo')

##     tams.data <- calvadrscripts::load.tams.from.csv(tams.site=tams.site,
##                                     year=year,
##                                     tams.path=tams.path)
##     tams.data <- calvadrscripts::reshape.tams.from.csv(tams.csv=tams.data,
##                                        year=year,
##                                        tams.path = tams.path)

##     testthat::expect_type(tams.data,'list')

##     site.lanes <- tams.data[[2]]
##     testthat::expect_equal(site.lanes,5)

##     tams.data <- tams.data[[1]]
##     testthat::expect_type(tams.data,'list')


##     directions <- names(tams.data)
##     testthat::expect_that(sort(directions),testthat::equals(c('E','W')))

##     direction <- 'E'
##     cdb.tamsid <- paste('tams',tams.site,direction,sep='.')

##     attach.files <- calvadrscripts::plot_tams.data(tams.data[[direction]]
##                                   ,site_no=tams.site
##                                   ,direction=direction
##                                   ,year=year
##                                   ,lanes.count=site.lanes
##                                   ,fileprefix='raw'
##                                   ,subhead='\npre imputation'
##                                   ,force.plot=TRUE
##                                   ,trackingdb=parts
##                                   ,tams.path=tams.path)


##     testthat::expect_equal(attach.files,
##                            paste(tams.path,'/'
##                                 ,year,'/'
##                                 ,tams.site,'/'
##                                 ,direction,'/'
##                                 ,'images/'
##                                 ,tams.site,'_'
##                                 ,direction,'_'
##                                 ,year
##                                 ,'_raw_',
##                                  c("001.png",
##                                    "002.png",
##                                    "003.png",
##                                    "004.png",
##                                    "005.png",
##                                    "006.png"),
##                                  sep='')
##                            )

##     for(f2a in c(attach.files)){
##         result <- rcouchutils::couch.attach(parts,cdb.tamsid,f2a)
##         testthat::expect_equal(result$ok,TRUE)
##         testthat::expect_equal(result$id,cdb.tamsid)
##     }
##     ## need to check that saved okay with this second call

##     attach.files <- calvadrscripts::plot_tams.data(tams.data[[direction]]
##                                   ,site_no=tams.site
##                                   ,direction=direction
##                                   ,year=year
##                                   ,lanes.count=site.lanes
##                                   ,fileprefix='raw'
##                                   ,subhead='\npre imputation'
##                                   ,force.plot=FALSE
##                                   ,trackingdb=parts
##                                   ,tams.path=tams.path)

##     ## it should have bailed out
##     testthat::expect_equal(attach.files,1)

##     ## make sure force.plot flag works
##     attach.files <- calvadrscripts::plot_tams.data(tams.data[[direction]]
##                                   ,site_no=tams.site
##                                   ,direction=direction
##                                   ,year=year
##                                   ,lanes.count=site.lanes
##                                   ,fileprefix='raw'
##                                   ,subhead='\npre imputation'
##                                   ,force.plot=TRUE
##                                   ,trackingdb=parts
##                                   ,tams.path=tams.path)

##     testthat::expect_equal(attach.files,
##                            paste(tams.path,'/'
##                                 ,year,'/'
##                                 ,tams.site,'/'
##                                 ,direction,'/'
##                                 ,'images/'
##                                 ,tams.site,'_'
##                                 ,direction,'_'
##                                 ,year
##                                 ,'_raw_',
##                                  c("001.png",
##                                    "002.png",
##                                    "003.png",
##                                    "004.png",
##                                    "005.png",
##                                    "006.png"),
##                                  sep='')
##                            )

##     context('Amelia call')

##     plotspath <- calvadrscripts::plot_path(tams.path = tams.path
##                                           ,year = year
##                                           ,site_no = tams.site
##                                           ,direction = direction
##                                           ,makedir = FALSE)
##     plotsname <- paste(plotspath,'ameliaplots.png',sep='/')
##     df.tams.amelia <- fill.tams.gaps(df.tams=tams.data[[direction]]
##                                     ,plotfile=plotsname)

##     testthat::expect_s3_class(df.tams.amelia,'amelia')
##     testthat::expect_equal(df.tams.amelia$code,1)


##     amelia.plots <- dir(plotspath,pattern='ameliaplots'
##                        ,full.names=TRUE,all.files=TRUE)
##     for(f2a in c(amelia.plots)){
##         result <- rcouchutils::couch.attach(parts,cdb.tamsid,f2a)
##         testthat::expect_equal(result$ok,TRUE)
##         testthat::expect_equal(result$id,cdb.tamsid)
##     }

##     calvadrscripts::store.amelia.chains(
##                         df.amelia=df.tams.amelia,
##                         year=year,
##                         detector.id=cdb.tamsid,
##                         imputation.name='tamsraw',
##                         maxiter=100,
##                         db=parts
##                     )

##     ## verify
##     result <- rcouchutils::couch.check.state(year=year,id=cdb.tamsid
##                                             ,state='tamsraw_chain_lengths'
##                                             ,db=parts)

##     testthat::expect_equal(length(result),5)
##     sapply(result, function (r){ testthat::expect_lt(r,100) } )


##     result <- rcouchutils::couch.check.state(year=year,id=cdb.tamsid
##                                             ,state='tamsraw_max_iterations'
##                                             ,db=parts)
##     testthat::expect_equal(result,0)

##     context('post process amelia result')
##     df.tams.agg.amelia <- tams.medianed.aggregate.df(df.tams.amelia)

##     testthat::expect_s3_class(df.tams.agg.amelia,'data.frame')
##     testthat::expect_equal(dim(df.tams.agg.amelia),c(2131,10))
##     testthat::expect_equal(sort(names(df.tams.agg.amelia)),
##                            c(
##                                "day"
##                               ,"heavyheavy_r1"
##                               ,"heavyheavy_r2"
##                               ,"hr"
##                               ,"n_r1"
##                               ,"n_r2"
##                               ,"not_heavyheavy_r1"
##                               ,"not_heavyheavy_r2"
##                               ,"tod"
##                               ,"ts"))



##     attach.files <- plot_tams.data(df.tams.agg.amelia,tams.site,direction,year
##                                   ,fileprefix='imputed'
##                                   ,lanes.count = site.lanes
##                                   ,subhead='\npost imputation'
##                                   ,force.plot=TRUE
##                                   ,trackingdb=parts
##                                   ,tams.path=tams.path)

##     testthat::expect_equal(attach.files,
##                            paste(tams.path,'/'
##                                 ,year,'/'
##                                 ,tams.site,'/'
##                                 ,direction,'/'
##                                 ,'images/'
##                                 ,tams.site,'_'
##                                 ,direction,'_'
##                                 ,year
##                                 ,'_imputed_',
##                                  c("001.png",
##                                    "002.png",
##                                    "003.png",
##                                    "004.png",
##                                    "005.png",
##                                    "006.png"),
##                                  sep='')
##                            )

##     for(f2a in c(attach.files)){
##         result <- rcouchutils::couch.attach(parts,cdb.tamsid,f2a)
##         testthat::expect_equal(result$ok,TRUE)
##         testthat::expect_equal(result$id,cdb.tamsid)
##     }
##     ## need to check that saved okay with this second call
##     attach.files <- plot_tams.data(df.tams.agg.amelia,tams.site,direction,year
##                                   ,fileprefix='imputed'
##                                    ,lanes.count = site.lanes
##                                   ,subhead='\npost imputation'
##                                   ,force.plot=FALSE
##                                   ,trackingdb=parts
##                                   ,tams.path=tams.path)
##     ## it should have bailed out
##     testthat::expect_equal(attach.files,1)

##     ## clean up for next test
##     rcouchutils::couch.delete(db=parts,docname=cdb.tamsid)
##     rcouchutils::couch.deletedb(parts)
## })

testthat::test_that("process tams  site also works okay",{
    parts <- c('tams','process_site')
    result <- rcouchutils::couch.makedb(parts)
    testthat::expect_equal(result$ok,TRUE)

    drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_rdatas)>0) {
        unlink(drop_rdatas)
    }
    drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
    if(length(drop_pngs)>0) {
        unlink(drop_pngs)
    }

    list.df.tams.amelia <- process.tams.site(tams.site=tams.site,
                                             year=year,
                                             seconds=seconds,
                                             preplot=TRUE,
                                             postplot=TRUE,
                                             force.plot=FALSE,
                                             tams.path=tams.path,
                                             trackingdb=parts
                                           )
    testthat::expect_type(list.df.tams.amelia,'list')
    directions <- names(list.df.tams.amelia)
    testthat::expect_equal(sort(directions),c('E','W'))
    for(direction in directions){
        testthat::expect_equal(list.df.tams.amelia[[direction]]$code,1)
        cdb.tamsid <- paste('tams',tams.site,direction,sep='.')
        doc <- rcouchutils::couch.get(parts,cdb.tamsid)
        attachments <- doc[['_attachments']]
        testthat::expect_type(attachments,'list')
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
                    sep='_')
             ,paste('ameliaplots_00',c(1,2),'.png',sep='')
              )
            )
    }

    ## also verify that you can load an Amelia file directly
    list.df.tams.amelia.2 <- process.tams.site(tams.site=tams.site,
                                               year=year,
                                               seconds=seconds,
                                               preplot=FALSE,
                                               postplot=TRUE,
                                               force.plot=TRUE,
                                               impute = FALSE,
                                               tams.path=tams.path,
                                               trackingdb=parts
                                               )

    testthat::expect_equal(list.df.tams.amelia
                           ,list.df.tams.amelia.2)

    rcouchutils::couch.delete(db=parts,docname=cdb.tamsid)
    rcouchutils::couch.deletedb(parts)
})

drop_rdatas <- dir(tams.data.path,pattern='RData$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
if(length(drop_rdatas)>0) {
    unlink(drop_rdatas)
}
drop_pngs <- dir(tams.data.path,pattern='png$',all.files=TRUE,full.names=TRUE,recursive=TRUE,ignore.case=TRUE,include.dirs=TRUE)
if(length(drop_pngs)>0) {
    unlink(drop_pngs)
}
