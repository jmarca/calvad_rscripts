source('get_couch.R')

load.wim.pair.data <- function(wim.ids,vds.nvars,year=0,localcouch=TRUE,do.couch.set.state=FALSE){
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
            result <- couch.get.attachment(trackingdb
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
        couch.set.state(year
                        ,vds.id
                        ,list('wim_neighbors_ready'=ready.wimids)
                        ,local=localcouch)
    }

    bigdata
}
