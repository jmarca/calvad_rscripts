## some specialized scripts to get things from couchdb 
source('./components/jmarca-rstats_couch_utils/couchUtils.R')

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
