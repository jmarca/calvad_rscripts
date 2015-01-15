library(testthat)
source('../lib/load.wim.file.R',chdir=TRUE)

wim.path <- "./data"
test_that("can get WIM RData file okay",{
    df.wim <- get.wim.rdata(wim.path=wim.path
                       ,year=2010
                       ,wim.site=108
                       ,direction='N')
    expect_that(is.data.frame(df.wim),is_true())
    expect_that(dim(df.wim),equals(c(8760,27)))
    expect_that(sort(names(df.wim)),equals(c(
        "count_all_veh_speed_l1"   ,"count_all_veh_speed_r1"
      , "day"                      ,"heavyheavy_l1"
      , "heavyheavy_r1"            ,"hh_axles_l1"
      , "hh_axles_r1"              ,"hh_len_l1"
      , "hh_len_r1"                ,"hh_speed_l1"
      , "hh_speed_r1"              ,"hh_weight_l1"
      , "hh_weight_r1"             ,"nh_axles_l1"
      , "nh_axles_r1"              ,"nh_len_l1"
      , "nh_len_r1"                ,"nh_speed_l1"
      , "nh_speed_r1"              ,"nh_weight_l1"
      , "nh_weight_r1"             ,"not_heavyheavy_l1"
      , "not_heavyheavy_r1"        ,"tod"
      , "ts"                       ,"wgt_spd_all_veh_speed_l1"
      , "wgt_spd_all_veh_speed_r1")))
})
