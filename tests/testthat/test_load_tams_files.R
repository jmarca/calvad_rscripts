config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

site_no <- 7005
direction <- 'S'
year <- 2012
tams.path='files'
test_that(
    'can load stored, aggregated TAMS from RData file',
    {
        df.tams <- load.tams.from.csv(
            tams.site=site_no
           ,year=year
           ,direction=direction
           ,tams.path=tams.path
        )

        expect_is(df.tams,'data.frame')
        expect_that(dim(df.tams),equals(c(8784,29)))
        expect_that(sort(names(df.tams)),equals(
            c(
                'count_all_veh_speed_r1',
                'count_all_veh_speed_r2',
                'count_all_veh_speed_r3',
                'day',
                'heavyheavy_r1',
                'heavyheavy_r2',
                'hh_axles_r1',
                'hh_axles_r2',
                'hh_len_r1',
                'hh_len_r2',
                'hh_speed_r1',
                'hh_speed_r2',
                'hh_weight_r1',
                'hh_weight_r2',
                'nh_axles_r1',
                'nh_axles_r2',
                'nh_len_r1',
                'nh_len_r2',
                'nh_speed_r1',
                'nh_speed_r2',
                'nh_weight_r1',
                'nh_weight_r2',
                'not_heavyheavy_r1',
                'not_heavyheavy_r2',
                'tod',
                'ts',
                'wgt_spd_all_veh_speed_r1',
                'wgt_spd_all_veh_speed_r2',
                'wgt_spd_all_veh_speed_r3'
                )))
        expect_that(table(is.na(df.tams$heavyheavy_r1)),
                    equals(table(c(rep(FALSE,8784-801),rep(TRUE,801) ))))
    })

test_that(
    'can load stored, TAMS imputation output from RData file',
    {
        df.tams.amelia <- get.amelia.tams.file.local(site_no=site_no
                                                  ,year=year
                                                  ,direction=direction
                                                  ,path=tams.path
                                                   )
        expect_is(df.tams.amelia,'amelia')
        expect_that(df.tams.amelia$code,equals(1))
        expect_that(length(df.tams.amelia$imputations),equals(5))

        context('\nalso test that can merge the amelia output okay')
        df.merged <- condense.amelia.output(df.tams.amelia)
        expect_is(df.merged,'data.frame')

        expect_that(dim(df.merged),equals(c(8784,29)))

        expect_that(table(is.na(df.merged$heavyheavy_r1)),
                    equals(table(rep(FALSE,8784))))

    })
