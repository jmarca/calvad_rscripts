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
##' @param year the year
##' @param trackingdb the db to check, the usual "vdsdata\%2ftracking"
##' is default goto db here
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
##' @param year the year
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
##' @param year the year
##' @param vdsid the id
##' @param condition some string or list describing what is wrong
##' @param db the couchdb to save into, defaults to vdsdata\%2ftracking
##' @return the result of calling couch.set.state
##' @author James E. Marca
couch.record.unmet.conditions <- function(year,vdsid,condition,
                                          db='vdsdata%2ftracking'){
  problem <- list()
  print(paste('unmet condition',condition))
  problem[condition] <- 'unmet'
  rcouchutils::couch.set.state(year,id=vdsid,doc=problem,db=db)
}
