##' Load WIM VDS paired data
##'
##' This function will load up all of the WIM VDS paired datasets for
##' a given set of WIM site ids.  It will match the incoming numbers
##' of lanes as best as possible.  So if you have a site with 5 lanes,
##' as indicated by the "vds.vars" input parameter, then this will try
##' to use sites with 5 or more lanes.  If it can't find that, then it
##' will fall back on the site(s) with the most lanes.
##'
##' The goal is to make a uniform data set for the imputation step.
##' If the paired data sets are missing some lanes compared to the
##' target VDS site, then you'll have to remove those extra lanes
##' prior to calling Amelia, or Amelia will crash as it will have no
##' way to guess the most likely values for those lanes.  This is also
##' why I picked the strange lane numbering scheme.  A site with 4
##' lanes can be paired with a site with 3 lanes because left lanes
##' and right lanes are correct, and are labeled the same way.
##'
##' @title load.wim.pair.data
##' @param wim.ids a vector of WIM ids that are to be used to impute
##' trucks at some site
##' @param vds.nvars the VDS count variables from the target site,
##' used to limit the chosen set of matched WIM-VDS paired sites
##' @param year
##' @param localcouch TRUE if you want to use the localhost couchdb,
##' FALSE if you want to use the vanilla and ostensibly remote
##' couchdb.  Usually TRUE is good if you have replication running
##' @param do.couch.set.state TRUE or FALSE, TRUE will set a "state"
##' in couchdb for the passed in vds.id, setting the list of
##' "wim_neighbors" that are ready to go for this year.
##' @param vds.id Optional, but required if do.couch.set.state is
##' TRUE.  The VDS id of the detector that will be the target of the
##' truck imputation step.  For setting the state with the list of WIM
##' neighbors that are ready to go (or not)
##' @return the "big data" dataframe of combined WIM and VDS sites,
##' trimmed to the right number of lanes
##' @author James E. Marca
##' @export
load.wim.pair.data <- function(wim.ids,vds.nvars,year=0,localcouch=TRUE,do.couch.set.state=FALSE,vds.id){
    vds.lanes <- length(vds.nvars)
    wim.vds.pairs <- get.vds.wim.pairs(year)
    if(dim(wim.vds.pairs)[1]==0) {
        print(paste('no wim pairs for year',year))
        stop()
    }

    bigdata <- data.frame()
    if(length(wim.ids)<1){
        print('no wim neighbors in database')
        ## couch.set.state(year,vds.id,list('truck_imputation_failed'='0 records in wim neighbor table'),local=localcouch)
        stop()
    }
    ## keep either the max number of lanes group, or all sites that have
    ## more or equal to number of lanes
    more.lanes <- wim.ids$lanes >= vds.lanes
    if(length(wim.ids[more.lanes,1])<1){
        ## then just use the max lanes group
        maxlanes = wim.ids$lanes[1]
        more.lanes <- wim.ids$lanes >= maxlanes
    }
    wim.ids <- wim.ids[more.lanes,]
    wim.lanes <- wim.ids$lanes[1]
    ready.wimids = list()
    spd.pattern <- "(^sl1$|^sr\\d$)"

    checked.already=c(1)
    for( wii in 1:length(wim.ids$wim_id) ){
        wim.id <- wim.ids[wii,'wim_id']
        wim.dir <- wim.ids[wii,'direction']

        paired.vdsids <- wim.vds.pairs[wim.vds.pairs$wim_id==wim.id  & wim.vds.pairs$direction==wim.dir,'vds_id']
        print(paired.vdsids)

        ## probably need to replace this looping action with a single
        ## function, called lapply

        for(paired.vdsid in paired.vdsids){
            if(paired.vdsid %in% checked.already) { next() }
            checked.already <- c(checked.already,paired.vdsid)

            paired.RData <- get.RData.view(paired.vdsid,year)
            if(length(paired.RData)==0) { next() }
            result <- rcouchutils::couch.get.attachment(trackingdb
                                           ,paired.vdsid
                                           ,paired.RData
                                           ,local=localcouch)
            if(result != "df.merged"){
                print (paste(paired.vdsid,paired.RData,'not df.merged'))
                next()
            }
            if(dim(df.merged)[1] < 100){
                print(paste('pairing for',paired.vdsid,paired.RData,'pretty empty'))
                next()
            }

            ## trim off some variables
            df.trimmed <- evaluate.paired.data(df.merged
                                               ,wim.lanes=wim.lanes
                                               ,vds.lanes=vds.lanes)
            rm(df.merged,pos=1)  ## actually, this prevents bugs
            ## pos=1 is needed because df.merged is loaded into .GlobalEnv
            ## by the couchdb fetch code

            df.trimmed$vds_id <- paired.vdsid

            print(wim.ids[wii,])
            ready.wimids[[length(ready.wimids)+1]]=wim.ids[wii,]

            if(length(bigdata)==0){
                bigdata <-  df.trimmed
            }else{
                ## here I need to make sure all WIM-VDS sites have similar lanes
                ## the concern is a site with *fewer* lanes than the vds site
                ic.names <- names(df.trimmed)
                bigdata.names <- names(bigdata)
                ## keep the larger of the two
                common.names <- intersect(ic.names,bigdata.names)
                bigdata <- bigdata[,common.names]
                df.trimmed <- df.trimmed[,common.names]
                bigdata <- rbind( bigdata, df.trimmed )
            }
        }
    }
    if(length(ready.wimids)>0 && do.couch.set.state){
        ready.wimids <- unique(ready.wimids)
        rcouchutils::couch.set.state(year
                        ,vds.id
                        ,list('wim_neighbors_ready'=ready.wimids)
                        ,local=localcouch)
    }

    bigdata
}
