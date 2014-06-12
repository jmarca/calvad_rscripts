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
        for(paired.vdsid in paired.vdsids){
            if(paired.vdsid %in% checked.already) { next }
            checked.already <- c(checked.already,paired.vdsid)
            paired.RData <- get.RData.view(paired.vdsid,year)
            if(length(paired.RData)==0) { next }
            result <- couch.get.attachment(trackingdb,paired.vdsid,paired.RData,local=localcouch)
            ## load(result)
            ## loading now happens in couch.get.attachment
            df.merged <- tempfix.borkborkbork(df.merged)
            if(dim(df.merged)[1] < 100){
                print(paste('pairing for',paired.vdsid,year,'pretty empty'))
                next
            }
            ic.names <- names(df.merged)

            ## this is an ugly hack that needs fixing
            ##
            ## I go through the variables I expect, and if they aren't
            ## there, I bail, but instead of looking up each name
            ## individually, which is possible, I instead just sum up
            ## the expected number of variables, which again, is a
            ## hack and needs fixing
            ##
            ## I don't even know for a fact that the summary report
            ## speed and count data is even used in the imputations?
            ##
            ## I mean, those vars are *in* the imputation, but I don't
            ## use the output, I use the truck counts and such.  these
            ## vars are only in to help the imputation move along
            ## nicely and be consistent with itself.

            shouldbe <-
                (wim.lanes * 2) + ## n and o for each lane
                    (2 * 10) +  ## two right hand lanes should have 10 truck vars
                        (wim.lanes * 2) + ## each lane has speed and count from summary report
                            3 ## ts, tod, day
            minlanes <- min(2,wim.lanes)
            atleast <-
                (wim.lanes * 2) + ## n and o for each lane
                    (2 * 10) +  ## two right hand lanes should have 10 truck vars
                        (minlanes * 2) + ## at the very least, need two lanes lane has speed and count from summary report
                            3 ## ts, tod, day
            ## add for speed too if in the data set
            speed.vars <- grep( pattern=spd.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=FALSE)
            if(length(speed.vars)>0){
                shouldbe <- shouldbe + (wim.lanes) # one speed measurement for each lane
                atleast  <- atleast  + (wim.lanes) # one speed measurement for each lane
            }

            if(dim(df.merged)[2]<shouldbe ){
                print(paste('pairing for',paired.vdsid,year,'missing some variables, expected',shouldbe,'got',dim(df.merged)[2]))
                print(names(df.merged))
                if(dim(df.merged)[2]<atleast ){
                    print(paste('pairing for',paired.vdsid,year,'missing less than minimum acceptable variables, require',atleast,'got',dim(df.merged)[2]))
                    next
                }
            }
            print(paste('processing',paired.vdsid,year))
            ready.wimids[length(ready.wimids)+1]=wim.ids[wii,]
            ## convention over configuration I guess.  These files are always called df.merged
            keep.columns = intersect(c( "ts","tod","day","imp","vds_id" ),ic.names)
            ## use vds.nvars to drop unwanted lanes
            for( lane in 1:length(vds.nvars) ){
                pattern = paste(substring(vds.nvars[lane],2)[1],'$',sep='')
                keep.columns.lane <-  grep( pattern=pattern,x=ic.names,perl=TRUE,value=TRUE)
                if(length(keep.columns) == 0){
                    keep.columns = keep.columns.lane
                }else{
                    keep.columns = union(keep.columns,keep.columns.lane)
                }
            }
            df.merged <- df.merged[,keep.columns]
            ## merge it
            if(length(bigdata)==0){
                bigdata <-  df.merged
            }else{
                ## here I need to make sure all WIM-VDS sites have similar lanes
                ## the concern is a site with *fewer* lanes than the vds site
                ic.names <- names(df.merged)
                bigdata.names <- names(bigdata)
                ## keep the larger of the two
                common.names <- intersect(ic.names,bigdata.names)
                bigdata <- bigdata[,common.names]
                df.merged <- df.merged[,common.names]
                bigdata <- rbind( bigdata, df.merged )
            }
            rm(df.merged)

        }
    }
    if(length(ready.wimids)>0){
        ready.wimids <- unique(ready.wimids)
      couch.set.state(year,vds.id,list('wim_neighbors_ready'=ready.wimids),local=localcouch)
    }

  bigdata
}
