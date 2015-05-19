config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('vds','get','right','year')
result <- rcouchutils::couch.makedb(parts)


context('get the right year')

test_that(
    "get the right year",{
        vds.id <- 1202248
        path <- './files'

        year <- 2012
        aout <- get.amelia.vds.file.local(vdsid=vds.id,
                                          year=year,
                                          path=path)
        expect_that(aout,is_a('data.frame'))
        year <- 2011
        aout <- get.amelia.vds.file.local(vdsid=vds.id,
                                          year=year,
                                          path=path)
        expect_that(aout,equals('todo'))
        year <- 2010
        aout <- get.amelia.vds.file.local(vdsid=vds.id,
                                          year=year,
                                          path=path)
        expect_that(aout,equals('data.frame'))


    })

rcouchutils::couch.deletedb(parts)
