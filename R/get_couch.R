## some specialized scripts to get things from couchdb

## now I've converted to a proper R package, of sorts

##' get vds wim pairs from couchdb
##'
##' get the design doc vds/pairRData for the passed in year
##'
##' The result will be the paired vds_id and wim_ids, suitable for use.
##'
##' The advantage of this routine is that the couchdb file *might*
##' contain more up to date information about the usability of vds or
##' wim data, compared to the postgresql data table that does not
##' contain any information on how good the data is.
##'
##' @title get.vds.wim.pairs
##' @param year
##' @param trackingdb
##' @return a data frame with year, vds_id, wim_id, doc
##' @author James E. Marca
get.vds.wim.pairs <- function(year,trackingdb='vdsdata%2ftracking'){
    docs <- rcouchutils::couch.allDocs(db=trackingdb
                                      ,query=list(
                            'startkey'=paste('%5b%22',year,'%22','%5d',sep='')
                            ,'endkey' =paste('%5b%22',year+1,'%22','%5d',sep='')
                            ,'reduce'='false')
                                      ,view='_design/vds/_view/pairRData'
                                      ,include.docs = FALSE)
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

##' get RData view
##'
##' I sometimes store RData as attachments to docs in couchdb
##'
##' @title get.RData.view
##' @param vdsid the VDS id
##' @param year
##' @param trackingdb defaults to 'vdsdata/tracking'
##' @return the files, which is the rows from the view Actually right
##' now as I write this documentation, I forget what this actually
##' returns.
##' @author James E. Marca
get.RData.view <- function(vdsid,year,trackingdb='vdsdata%2ftracking'){
    docs <- rcouchutils::couch.allDocs(
        trackingdb
       ,query=list(
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

##' Record unmet imputation conditions in CouchDB
##'
##' Rather than skipping a detector and moving on, instead write in
##' couchdb tracking database why the detector could not be imputed
##'
##' I can't find where this is actually used at the moment.
##'
##' @title couch.record.unmet.conditions
##' @param year
##' @param vdsid
##' @param condition some string or list describing what is wrong
##' @return the result of calling couch.set.state
##' @author James E. Marca
couch.record.unmet.conditions <- function(district,year,vdsid,condition){
  problem <- list()
  print(paste('unmet condition',condition))
  problem[condition] <- 'unmet'
  rcouchutils::couch.set.state(year,vdsid,doc=problem,local=TRUE)
}

##' evaluate paired data
##'
##' This function fixed an even uglier hack done earlier for speed.
##' The logic is to sift through the names, keep what I need, discard
##' (?) what I don't wim data, needs to have wim.lanes worth of info
##'
##' @title evaluate.paired.data
##' @param df the data frame with paired data
##' @param wim.lanes lanes at the WIM site
##' @param vds.lanes lanes at the VDS site
##' @return a dataframe that equals
##' df[,c(vds.vars.lanes,wim.vars.lanes,other.vars)] where
##' vds.vars.lanes is the vds variables (vol, occ), wim.vars.lanes is
##' the wim variabes (*hh, *weight,*axle, and *speed variables, see
##' the code for the exact), and other.vars are other variables
##' @author James E. Marca
evaluate.paired.data <- function(df,wim.lanes,vds.lanes){
    paired.data.names <- names(df)


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
