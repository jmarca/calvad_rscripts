library('testthat')
source('../lib/get_couch.R',chdir=TRUE)

test_that("get.wim.vds.pairs() will get all pairs for a given year", {

    year <- 2010
    w.v.p <- get.vds.wim.pairs(year)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(87,5)))
    ## was going to check year == 2010, but why bother

    w.v.p <- get.vds.wim.pairs(2014)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(0,0)))

    w.v.p <- get.vds.wim.pairs(2007)
    expect_that(is.data.frame(w.v.p), is_true())
    expect_that(dim(w.v.p),equals(c(78,5)))

})

test_that("get.Rdata.view works okay",{

    vdsid <- 1114696
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    expect_that(paired.attachment,equals("wim.37.S.vdsid.1114696.2010.paired.RData"))

    vdsid <- 1114694
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    expect_that(paired.attachment,is_equivalent_to(list()))

})


## this is really another bit of library, but so what
## high level function is used here
test_that("can get attached RData file okay",{

    vdsid <- 1114696
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    result <- couch.get.attachment('vdsdata%2ftracking'
                                   ,vdsid
                                   ,paired.attachment
                                   ,local=FALSE)
    expect_that(result,equals("df.merged"))
    expect_that(is.null(df.merged),is_false())
    expect_that(is.data.frame(df.merged),is_true())
    expect_that(dim(df.merged),equals(c(8755,37)))

})

## this test checks being able to filter out lanes
##
## same exact parameters in the setup stageas the above test, because
## it that works, then this will work
test_that("can filter out lanes from merged dataframe",{
    vdsid <- 1114696
    year <- 2010
    paired.attachment <- get.RData.view(vdsid,year)
    result <- couch.get.attachment('vdsdata%2ftracking'
                                   ,vdsid
                                   ,paired.attachment
                                   ,local=FALSE)

    paired.names <- sort(names(df.merged))
    ## this set originally has three lanes in vds data, two lanes in
    ## WIM truck data, plus a third lane in report data for speeds

    ## in this case, I expect the exact same column names in both sets
    df.trimmed <- evaluate.paired.data(df.merged,3,4)
    expect_that(is.data.frame(df.merged),is_true())
    expect_that(dim(df.trimmed),equals(c(8755,37)))
    expect_that(sort(names(df.trimmed)),equals(paired.names))

    ## now fewer lanes
    df.trimmed <- evaluate.paired.data(df.merged,2,4)
    ## in this case, I expect the exact same column names in both sets
    expect_that(is.data.frame(df.merged),is_true())
    expect_that(dim(df.trimmed),equals(c(8755,35)))
    expected.names <- c(
        "nl1"                      ,"nr3"
        ,"nr2"                      ,"nr1"
        ,"ol1"                      ,"or3"
        ,"or2"                      ,"or1"
        ,"not_heavyheavy_r1"        ,"heavyheavy_r1"
        ,"hh_weight_r1"             ,"hh_axles_r1"
        ,"hh_len_r1"                ,"hh_speed_r1"
        ,"nh_weight_r1"             ,"nh_axles_r1"
        ,"nh_len_r1"                ,"nh_speed_r1"
        ,"not_heavyheavy_r2"        ,"heavyheavy_r2"
        ,"hh_weight_r2"             ,"hh_axles_r2"
        ,"hh_len_r2"                ,"hh_speed_r2"
        ,"nh_weight_r2"             ,"nh_axles_r2"
        ,"nh_len_r2"                ,"nh_speed_r2"
        ,"wgt_spd_all_veh_speed_r1" ,"count_all_veh_speed_r1"
        ,"wgt_spd_all_veh_speed_r2" ,"count_all_veh_speed_r2"
        ,"ts"                       ,"tod"
        ,"day")
    expect_that(sort(names(df.trimmed)),equals(sort(expected.names)))

    ## now just 2 lanes of general flow
    df.trimmed <- evaluate.paired.data(df.merged,2,2)
    ## in this case, I expect the exact same column names in both sets
    expect_that(is.data.frame(df.merged),is_true())
    expect_that(dim(df.trimmed),equals(c(8755,31)))
    expected.names <- c(
        "nl1"
        ,"nr1"
        ,"ol1"
        ,"or1"
        ,"not_heavyheavy_r1"        ,"heavyheavy_r1"
        ,"hh_weight_r1"             ,"hh_axles_r1"
        ,"hh_len_r1"                ,"hh_speed_r1"
        ,"nh_weight_r1"             ,"nh_axles_r1"
        ,"nh_len_r1"                ,"nh_speed_r1"
        ,"not_heavyheavy_r2"        ,"heavyheavy_r2"
        ,"hh_weight_r2"             ,"hh_axles_r2"
        ,"hh_len_r2"                ,"hh_speed_r2"
        ,"nh_weight_r2"             ,"nh_axles_r2"
        ,"nh_len_r2"                ,"nh_speed_r2"
        ,"wgt_spd_all_veh_speed_r1" ,"count_all_veh_speed_r1"
        ,"wgt_spd_all_veh_speed_r2" ,"count_all_veh_speed_r2"
        ,"ts"                       ,"tod"
        ,"day")
    expect_that(sort(names(df.trimmed)),equals(sort(expected.names)))


})
