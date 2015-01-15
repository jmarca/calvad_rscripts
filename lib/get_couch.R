## some specialized scripts to get things from couchdb
source('../node_modules/rstats_couch_utils/couchUtils.R')

get.vds.wim.pairs <- function(year,trackingdb='vdsdata%2ftracking'){
  docs <- couch.allDocs(trackingdb
                        , query=list(
                            'startkey'=paste('%5b%22',year,'%22','%5d',sep='')
                            ,'endkey' =paste('%5b%22',year+1,'%22','%5d',sep='')
                            ,'reduce'='false')
                        , view='_design/vds/_view/pairRData'
                        , include.docs = FALSE)
  rows <- docs$rows
  records <- sapply(rows,function(r){
      ## parse out wim info
      x = r$key[[3]]
      m <- regexec("^wim\\.([0-9]+)\\.([NSEW])",x)
      wim.info <- regmatches(x,m)[[1]]
      return (list('year'=as.numeric(r$key[[1]]),
                   'vds_id'=as.numeric(r$key[[2]]),
                   'doc'=r$key[[3]],
                   'wim_id'=as.integer(wim.info[2]),
                   'direction'=wim.info[3]
                   ))
  })
  if(length(records)==0){
      return(data.frame())
  }
  ## convert to a dataframe
  df.pairs <- data.frame(year=unlist(records[1,])
                        ,vds_id=unlist(records[2,])
                        ,wim_id=unlist(records[4,])
                        ,direction=unlist(records[5,])
                        ,doc=unlist(records[3,])
                        ,stringsAsFactors=FALSE)
  df.pairs
}

get.RData.view <- function(vdsid,year,trackingdb='vdsdata%2ftracking'){
    docs <- couch.allDocs(trackingdb
                          , query=list(
                            'startkey'=paste('%5b%22',year,'%22,%22',vdsid,'%22%5d',sep='')
                            ,'endkey' =paste('%5b%22',year,'%22,%22',vdsid,'%22,%5b%5d%5d',sep='')
                            ,'reduce'='false')
                        , view='_design/vds/_view/pairRData'
                        , include.docs = FALSE)
  rows <- docs$rows
  files <- sapply(rows,function(r){
    return (r$key[[3]])
  })
  files
}

couch.record.unmet.conditions <- function(district,year,vdsid,condition){
  problem <- list()
  print(paste('unmet condition',condition))
  problem[condition] <- 'unmet'
  couch.set.state(year,vdsid,doc=problem,local=TRUE)
}


evaluate.paired.data <- function(df,wim.lanes,vds.lanes){
    paired.data.names <- names(df)

    ## fixing an ugly hack done earlier for speed.

    ## sift through the names, keep what I need, discard (?) what I don't

    ## wim data, needs to have wim.lanes worth of info

    wim.var.pattern <-
        "(heavyheavy|_weight|_axle|_len|_speed)"
    ## "(heavyheavy|_weight|_axle|_len|_speed|_all_veh_speed)"

    wim.vars <- grep(pattern=wim.var.pattern,x=paired.data.names
                     ,perl=TRUE,value=TRUE)
    other.vars <- grep(pattern=wim.var.pattern,x=paired.data.names
                       ,perl=TRUE,value=TRUE,invert=TRUE)
    lanes.vars <- c()
    for(lane in 1:wim.lanes){
        lane.pattern <- paste("r",lane,sep='')
        lane.vars <- grep(pattern=lane.pattern,x=wim.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- c(lanes.vars,lane.vars)
    }
    wim.vars.lanes <- lanes.vars


    vds.var.pattern <- "(^nl|^nr\\d|^ol|^or\\d)"
    vds.vars <- grep(pattern=vds.var.pattern,x=paired.data.names
                     ,perl=TRUE,value=TRUE)
    other.vars <- grep(pattern=vds.var.pattern,x=other.vars
                       ,perl=TRUE,value=TRUE,invert=TRUE)

    ## expect_that(sort(c(other.vars
    ##                    ,vds.vars,wim.vars))
    ##             ,equals(sort(paired.data.names)))
    ## passed in testing

    ## reset
    lanes.vars <- c()

    ## need to process lanes right and left lane separately right lane
    ## is numbered from the right as r1 to r(n-1).  The left lane is
    ## always numbered l1.  If a site has one lane, that lane, by
    ## definition, is r1.  If a site has two lanes, the lanes are r1
    ## and l1, again, by definition.  If a site has three lanes, (n=3)
    ## then the left lane is l1, and the other lanes are numbered r1
    ## and r2, AKA r(n-1)

    ## so special case is n=1 (right lane only)
    right.lanes <- vds.lanes
    if(vds.lanes>1){
        ## there *is* a left lane for all cases when n>1
        right.lanes <- vds.lanes-1
        lane.pattern <- "l1"
        lane.vars <- grep(pattern=lane.pattern,x=vds.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- lane.vars
    }
    for(lane in 1:right.lanes){
        ## in the case when vds.lanes==1, right.lanes also == 1
        lane.pattern <- paste("r",lane,sep='')
        lane.vars <- grep(pattern=lane.pattern,x=vds.vars
                          ,perl=TRUE,value=TRUE)
        lanes.vars <- c(lanes.vars,lane.vars)
    }
    vds.vars.lanes <- lanes.vars

    pared.df <- df[,c(vds.vars.lanes,wim.vars.lanes,other.vars)]
    pared.df
}
