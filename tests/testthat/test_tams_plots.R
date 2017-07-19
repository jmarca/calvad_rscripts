config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('tams','plots')
result <- rcouchutils::couch.makedb(parts)

context('building blocks of process.tams.site work okay')
test_that("plotting tams data code works okay",{

    file <- './files/2012/87/S/tams.agg.RData'
    tams.path <- './files'
    res <- load(file)
    expect_that(res,equals('df.tams.d.joint'))

    varnames <- names(df.tams.d.joint)
    expect_that(
        varnames,
        equals(
            c(
                "ts"                       ,"not_heavyheavy_r1"
               ,"heavyheavy_r1"            ,"hh_weight_r1"
               ,"hh_axles_r1"              ,"hh_len_r1"
               ,"hh_speed_r1"              ,"nh_weight_r1"
               ,"nh_axles_r1"              ,"nh_len_r1"
               ,"nh_speed_r1"              ,"not_heavyheavy_r2"
               ,"heavyheavy_r2"            ,"hh_weight_r2"
               ,"hh_axles_r2"              ,"hh_len_r2"
               ,"hh_speed_r2"              ,"nh_weight_r2"
               ,"nh_axles_r2"              ,"nh_len_r2"
               ,"nh_speed_r2"              ,"not_heavyheavy_r3"
               ,"heavyheavy_r3"            ,"hh_weight_r3"
               ,"hh_axles_r3"              ,"hh_len_r3"
               ,"hh_speed_r3"              ,"nh_weight_r3"
               ,"nh_axles_r3"              ,"nh_len_r3"
               ,"nh_speed_r3"              ,"wgt_spd_all_veh_speed_l1"
               ,"count_all_veh_speed_l1"   ,"wgt_spd_all_veh_speed_r1"
               ,"count_all_veh_speed_r1"   ,"wgt_spd_all_veh_speed_r2"
               ,"count_all_veh_speed_r2"   ,"wgt_spd_all_veh_speed_r3"
               ,"count_all_veh_speed_r3"   ,"tod"
               ,"day"
            )))

    files.to.couch <- plot_tams.data(df.tams
                                   ,7005
                                   ,'E'
                                   ,2017
                                   ,fileprefix='raw'
                                   ,subhead='\npre imputation'
                                   ,force.plot=TRUE
                                   ,trackingdb=parts
                                   ,tams.path=tams.path)

    expect_that(files.to.couch,
                equals(
                    c(
                        './files/2012/87/S/images/87_S_2012_raw_001.png'
                       ,'./files/2012/87/S/images/87_S_2012_raw_002.png'
                       ,'./files/2012/87/S/images/87_S_2012_raw_003.png'
                       ,'./files/2012/87/S/images/87_S_2012_raw_004.png'
                       ,'./files/2012/87/S/images/87_S_2012_raw_005.png'
                       ,'./files/2012/87/S/images/87_S_2012_raw_006.png'
                    )))

    ## should also md5 check the dumped images?

    unlink(files.to.couch)
})

test_that("plotting tams data post-imputed code works okay",{

    file <- './files/2012/87/S/tams87S.3600.imputed.RData'
    tams.path <- './files'
    res <- load(file)
    expect_that(res,equals('df.tams.amelia'))

    df.tams.2 <- get.amelia.tams.file.local(site_no=87
                                         ,year=2012
                                         ,direction='S'
                                         ,path=tams.path)
    expect_that(df.tams.2,equals(df.tams.amelia))
    df.tams.agg.amelia <- tams.medianed.aggregate.df(df.tams.2)

    files.to.couch <- plot_tams.data(df.tams.agg.amelia
                                   ,87
                                   ,'S'
                                   ,2012
                                   ,fileprefix='imputed'
                                   ,subhead='\npost imputation'
                                   ,force.plot=TRUE
                                   ,trackingdb=result
                                   ,tams.path=tams.path)

    expect_that(files.to.couch,
                equals(
                    c(
                        './files/2012/87/S/images/87_S_2012_imputed_001.png'
                       ,'./files/2012/87/S/images/87_S_2012_imputed_002.png'
                       ,'./files/2012/87/S/images/87_S_2012_imputed_003.png'
                       ,'./files/2012/87/S/images/87_S_2012_imputed_004.png'
                       ,'./files/2012/87/S/images/87_S_2012_imputed_005.png'
                       ,'./files/2012/87/S/images/87_S_2012_imputed_006.png'
                    )))

    ## should also md5 check the dumped images?

    unlink(files.to.couch)
})


rcouchutils::couch.deletedb(parts)
