source('get_couch.R')

load.wim.pair.data <- function(wim.ids,vds.nvars,lanes=0,year=0){
    wim.vds.pairs <- get.vds.wim.pairs(year)
    if(dim(wim.vds.pairs)[1]==0) {
        print(paste('no wim pairs for year',year))
        stop()
    }

    bigdata <- data.frame()
    if(length(wim.ids)<1){
        print('no wim neighbors in database')
        couch.set.state(year,vds.id,list('truck_imputation_failed'='0 records in wim neighbor table'),local=localcouch)
        stop()
    }
    ## keep either the max number of lanes group, or all sites that have
    ## more or equal to number of lanes
    more.lanes <- wim.ids$lanes >= lanes
    if(length(wim.ids[more.lanes,1])<1){
        ## then just use the max lanes group
        maxlanes = wim.ids$lanes[1]
        more.lanes <- wim.ids$lanes >= maxlanes
    }
    wim.ids <- wim.ids[more.lanes,]
    ready.wimids = list()
    spd.pattern <- "(^sl1$|^sr\\d$)"

    checked.already=c(1)
    for( wii in 1:length(wim.ids$wim_id) ){
        wim.id <- wim.ids[wii,'wim_id']
        wim.dir <- wim.ids[wii,'direction']
        wim.lanes <- wim.ids[wii,'lanes']
        ## make sure that there are the *correct* nubmer of variables
        ## there should be 14 for each lane, and then 4 for the left
        ## lane, if there are more than two lanes, then 3 for time variables

        paired.vdsids <- wim.vds.pairs[wim.vds.pairs$wim_id==wim.id  & wim.vds.pairs$direction==wim.dir,'vds_id']
        print(paired.vdsids)

        ## need to replace this looping action with a single function, called lapply

        for(paired.vdsid in paired.vdsids){
            if(paired.vdsid %in% checked.already) { next }
            checked.already <- c(checked.already,paired.vdsid)

            paired.RData <- get.RData.view(paired.vdsid,year)
            if(length(paired.RData)==0) { next }
            result <- couch.get.attachment(trackingdb
                                           ,paired.vdsid
                                           ,paired.RData
                                           ,local=localcouch)
            if(dim(df.merged)[1] < 100){
                print(paste('pairing for',paired.vdsid,year,'pretty empty'))
                next
            }

            ## trim off some variables
            paired.R

        }
    }
    if(length(ready.wimids)>0){
        ready.wimids <- unique(ready.wimids)
      couch.set.state(year,vds.id,list('wim_neighbors_ready'=ready.wimids),local=localcouch)
    }

  bigdata
}
