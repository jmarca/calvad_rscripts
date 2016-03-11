config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))

parts <- c('wim','plots')
result <- rcouchutils::couch.makedb(parts)

context('building blocks of process.wim.site work okay')
test_that("plotting wim data code works okay",{

    file <- './files/2012/87/S/wim.agg.RData'
    wim.path <- './files'
    res <- load(file)
    expect_that(res,equals('df.wim.d.joint'))

    varnames <- names(df.wim.d.joint)
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

test_that("plotting wim data post-imputed code works okay",{

    file <- './files/2012/87/S/wim87S.3600.imputed.RData'
    wim.path <- './files'
    res <- load(file)
    expect_that(res,equals('df.wim.amelia'))

    df.wim.2 <- get.amelia.wim.file.local(site_no=87
                                         ,year=2012
                                         ,direction='S'
                                         ,path=wim.path)
    expect_that(df.wim.2,equals(df.wim.amelia))
    df.wim.agg.amelia <- wim.medianed.aggregate.df(df.wim.2)

    files.to.couch <- plot_wim.data(df.wim.agg.amelia
                                   ,87
                                   ,'S'
                                   ,2012
                                   ,fileprefix='imputed'
                                   ,subhead='\npost imputation'
                                   ,force.plot=TRUE
                                   ,trackingdb=result
                                   ,wim.path=wim.path)

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
