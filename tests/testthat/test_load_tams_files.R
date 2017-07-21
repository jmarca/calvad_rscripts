config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
tams.site <- 7005
year <- 2016
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'tests'
direction <- 'E'

testthat::test_that(
    'can load stored, aggregated TAMS from RData file',
    {
        tams.data.csv <- load.tams.from.csv(tams.site=tams.site,
                                            year=year,
                                            tams.path=tams.path)

        testthat::expect_is(tams.data.csv,'data.frame')
        testthat::expect_that(dim(tams.data.csv),testthat::equals(c(27921669,11)))
        testthat::expect_that(sort(names(tams.data.csv)),testthat::equals(
            c(
                "bc_group"
               ,"bc_id"
               ,"bc_name"
               ,"bcg_id"
               ,"calvad_class"
               ,"detstaid"
               ,"lane"
               ,"lane_dir"
               ,"sig_id"
               ,"timestamp_full"
               ,"vehicle_count"
                )))

    })

test_that(
    'can load stored, TAMS imputation output from RData file',
    {
        ## df.tams.amelia <- get.amelia.tams.file.local(site_no=site_no
        ##                                           ,year=year
        ##                                           ,direction=direction
        ##                                           ,path=tams.path
        ##                                            )
        ## expect_is(df.tams.amelia,'amelia')
        ## expect_that(df.tams.amelia$code,equals(1))
        ## expect_that(length(df.tams.amelia$imputations),equals(5))

        ## context('\nalso test that can merge the amelia output okay')
        ## df.merged <- condense.amelia.output(df.tams.amelia)
        ## expect_is(df.merged,'data.frame')

        ## expect_that(dim(df.merged),equals(c(8784,29)))

        ## expect_that(table(is.na(df.merged$heavyheavy_r1)),
        ##             equals(table(rep(FALSE,8784))))

    })
