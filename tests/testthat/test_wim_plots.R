config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('wim','plots')
result <- rcouchutils::couch.makedb(parts)

context('building blocks of process.wim.site work okay')
test_that("plotting wim data code works okay",{

    file <- './files/87/S/wim.agg.RData'
    wim.path <- './files/87/S'
    res <- load(file)
    expect_that(res,equals('df.wim.d.joint'))

    varnames <- names(df.wim.d.joint)
    expect_that(varnames,equals(c("ts", "nl1","nr3","nr2","nr1",
                                  "ol1", "or3","or2","or1",
                                  "obs_count","tod","day")))

    files.to.couch <- plot_wim.data(df.wim.d.joint
                                   ,87
                                   ,'S'
                                   ,2012
                                   ,fileprefix='raw'
                                   ,subhead='\npre imputation'
                                   ,force.plot=TRUE
                                   ,trackingdb=result
                                   ,wim.path=wim.path)

    expect_that(files.to.couch,
                equals(
                    c("images/718204/718204_2012_imputed_001.png",
                      "images/718204/718204_2012_imputed_002.png",
                      "images/718204/718204_2012_imputed_003.png",
                      "images/718204/718204_2012_imputed_004.png"
                      )))

    ## should also md5 check the dumped images?


})


rcouchutils::couch.deletedb(parts)
