config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
year <- 2016
seconds <- 3600
preplot <- TRUE
postplot <- TRUE
impute <- TRUE
force.plot <- FALSE
tams.path <- 'files'
direction <- 'E'

testthat::test_that(
    'can load TAMS from csv file',
    {
        tams.site <- 7005
        tams.data.csv <- load.tams.from.csv(tams.site=tams.site,
                                            year=year,
                                            tams.path=tams.path)
        testthat::expect_s3_class(tams.data.csv,'data.frame')
        testthat::expect_that(dim(tams.data.csv),testthat::equals(c(4636452,11)))

        matched <- tams.data.csv$detstaid == tams.site
        testthat::expect_equal(length(matched[matched]),length(matched))

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

testthat::test_that(
    'will not load the wrong csv file',
    {
        tams.site <- 7
        year <-  2016
        tams.data.csv <- load.tams.from.csv(tams.site=tams.site,
                                            year=year,
                                            tams.path=tams.path)

        testthat::expect_type(tams.data.csv,'list')
        testthat::expect_length(tams.data.csv,0)

        tams.site <- 62
        year <-  2017
        tams.data.csv <- load.tams.from.csv(tams.site=tams.site,
                                            year=year,
                                            tams.path=tams.path)

        testthat::expect_type(tams.data.csv,'list')
        testthat::expect_length(tams.data.csv,0)


        ## even if a stupid pattern, still won't keep data with wrong id
        filename.pattern <- paste(tams.site,'.*',year,'.*\\.(csv|csv.gz)$',sep='')
        tams.data.csv <- calvadrscripts::load.tams.from.csv(tams.site=tams.site,
                                            year=year,
                                            tams.path=tams.path,
                                            filename.pattern=filename.pattern
                                            )

        testthat::expect_type(tams.data.csv,'list')
        testthat::expect_s3_class(tams.data.csv,'data.frame')
        testthat::expect_length(tams.data.csv,11)
        testthat::expect_equals(dim(tams.data.csv),c(0,11))


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
