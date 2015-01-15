source('../lib/amelia_plots_and_diagnostics.R',chdir=TRUE)
source('../lib/get.medianed.amelia.vds.R',chdir=TRUE)
source('../lib/wim.loading.functions.R',chdir=TRUE)
source('../lib/paired.Rdata.R',chdir=TRUE)

library(testthat)

## must run this somewhere you can load vds data
test_that("load.wim.pair.data() will return a big data frame", {

    path <- '/data/backup/pems/breakup'
    year <- 2010
    ## fake entry, but real anyway
    ## 37 | 74135.4030687827 | south     |     4
    ## 87 | 23462.8692386871 | south     |     4

    ## fake pairing, but real anyway
    vdsid <- 1108541
    df.vds.zoo <- get.and.plot.vds.amelia(pair=vdsid,year,doplots=FALSE,remote=FALSE,path=path)
    expect_that(dim(df.vds.zoo),equals(c(8604,11)))

    df.vds <- unzoo.incantation(df.vds.zoo)
    expect_that(dim(df.vds.zoo),equals(c(8604,11)))

    rm(df.vds.zoo)
    df.vds[,'vds_id'] <- vdsid
    vds.names <- sort(names(df.vds))
    vds.nvars <- grep( pattern="^n(l|r)\\d+",x=vds.names,perl=TRUE,value=TRUE)

    ## loaded this site in R cursor, got
    wim.ids <- data.frame(wim_id=c(37,87)
                         ,direction=c('S','S')
                         ,lanes=c(4,4)
                         ,distance=c(74135.4,23462.9))

    bigdata <- load.wim.pair.data(wim.ids,vds.nvars=vds.nvars,year=year)
    expect_that(dim(bigdata),equals(c(34935,36)))


    wimsites.names <-  sort(names(bigdata))
    miss.names.wim <- setdiff(wimsites.names,vds.names)
    miss.names.vds <- setdiff(vds.names,wimsites.names)

    if(length(miss.names.vds)>0){
        bigdata[,miss.names.vds] <- NA
    }
    ## of course this will be necessary, as the wimsites have truck
    ## data and the vds does not
    df.vds[,miss.names.wim] <- NA

    ## merge into bigdata
    bigdata <- rbind(bigdata,df.vds)
    expect_that(dim(bigdata),equals(c(43539,38)))

    miss.names.vds <- union(miss.names.vds,c('vds_id'))
    i.hate.r <- c(miss.names.vds,'nr1') ## need a dummy index or R will simplify
    holding.pattern <- bigdata[,i.hate.r]

    this.vds <- bigdata['vds_id'] == vdsid
    this.vds <- !is.na(this.vds)  ## lordy I hate when NA isn't falsey

    for(i in miss.names.vds){
        bigdata[,i] <- NULL
    }
    rm(df.vds)

    expect_that(dim(bigdata),equals(c(43539,35)))

        occ.pattern <- "^o(l|r)\\d$"
    occ.vars <-  grep( pattern=occ.pattern,x=names(bigdata),perl=TRUE,value=TRUE)
    ## truncate mask
    toobig <-  !( bigdata[,occ.vars]<1 | is.na(bigdata[,occ.vars]) )
    bigdata[,occ.vars][toobig] <- 1

}
