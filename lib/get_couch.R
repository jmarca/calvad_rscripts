## some specialized scripts to get things from couchdb
source('../components/jmarca-rstats_couch_utils/couchUtils.R')

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
                   'vds.id'=as.numeric(r$key[[2]]),
                   'doc'=r$key[[3]],
                   'wim.id'=as.integer(wim.info[2]),
                   'direction'=wim.info[3]
                   ))
  })
  if(length(records)==0){
      return(data.frame())
  }
  ## convert to a dataframe
  df.pairs <- data.frame(year=unlist(records[1,])
                        ,vds.id=unlist(records[2,])
                        ,wim.id=unlist(records[4,])
                        ,direction=unlist(records[5,])
                        ,doc=unlist(records[3,])
                        ,stringsAsFactors=FALSE)
  df.pairs
}

get.RData.view <- function(vdsid,year){
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
