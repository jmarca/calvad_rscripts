## source('process.raw.pems.file.R')
## this script will impute hourly missing data for pems files

### but it is likely out of date

source("load.pems.raw.file.R")
##library('zoo')
####library('Hmisc')
##library('Amelia')
##library('lattice')
##library('RCurl')
##library('RJSONIO')

source("vds.processing.functions.R")
## source('../node_modules/rstats_couch_utils/couchUtils.R')
source('../node_modules/rstats_remote_files/remoteFiles.R')
server <- "http://calvad.ctmlabs.net"
vds.service <- 'vdsdata'
wim.service <- 'wimdata'
vds.path <- "/data/pems/breakup"
wim.path <- "/data/wim"
source('./get.medianed.amelia.vds.R')
source('./amelia_plots_and_diagnostics.R')

source('./vds_impute.R')
couch.check.is.raw.imputed <- function(year,vdsid){
  r <- rcouchtuils::couch.check.state(year,vdsid,'vdsraw_chain_lengths')
  result <- (r[1] != 'todo' )
  return (result)
}

## library('RPostgreSQL')
m <- dbDriver("PostgreSQL")
## requires environment variables be set externally
psqlenv = Sys.getenv(c("PSQL_HOST", "PSQL_USER", "PSQL_PASS"))

con <-  dbConnect(m
                  ,user=psqlenv[2]
                  ,host=psqlenv[1]
                  ,dbname="spatialvds")


reverse = as.numeric(Sys.getenv(c('RREVERSE'))[1])

process.yearly.files <- function(year,seconds=3600,base.dir="/data/pems/breakup",goodfactor=2,reconsider=FALSE){
  pattern <- paste("ML_",year,"\\.txt\\..z$",sep='')
  files <- dir(base.dir, pattern=pattern,all.files = TRUE,full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
  files <- sort(files,decreasing=(reverse==2))
  todo <- length(files)
  print ( paste ( "processing",todo,"files") )
  for( f in files){
    file.names <- strsplit(f,split="/")
    file.names <- file.names[[1]]
    path <- paste(file.names[1:(length(file.names)-1)],collapse="/")
    fname <-  strsplit(file.names[length(file.names)],"\\.")[[1]][1]

     vds.id <-  get.vdsid.from.filename(fname)

    if(couch.check.is.raw.imputed(year,vds.id)){
      ## really done
      todo <- todo - 1
    }else{
      print(paste('process',fname,f,path,year,seconds,goodfactor))
      done <- self.agg.impute.VDS.site.no.plots(fname,f,path,year,seconds=seconds,goodfactor=goodfactor)
      if(done == 1){
        todo <- todo - 1
        ##get.and.plot.vds.amelia(vds.id,year)
        print('quit after one and done')
        quit(save='no',status=10)
      }else{
          rcouchtuils::couch.set.state(year,
                                       vds.id,
                                       list('vdsraw_chain_lengths'=done))
      }
    }
  }
  return(todo)
}


year = as.numeric(Sys.getenv(c('RYEAR'))[1])
if(is.null(year)){
  print('assign the year to process to the RYEAR environment variable')
  exit(1)
}

goodfactor <-   3.5
seconds = 60


## districts<-  c(
##                '/data/pems/breakup/D12'
##                ,'/data/pems/breakup/D05'
##                ,'/data/pems/breakup/D06'
##                ,'/data/pems/breakup/D08'
##                ,'/data/pems/breakup/D03'
##                ,'/data/pems/breakup/D11'
##                ,'/data/pems/breakup/D07'
##                ,'/data/pems/breakup/D10'
##                ,'/data/pems/breakup/D04')
pemsfiles.dir = Sys.getenv(c('RDISTRICT'))[1]
if(is.null(pemsfiles.dir)){
  print('assign the district break up directory to the RDISTRICT environment variable')
  exit(1)
}

remaining <- 0
print(paste('year',year,'district',pemsfiles.dir))

remaining <- process.yearly.files(year,seconds,pemsfiles.dir,goodfactor,TRUE)
if(remaining == 0){
  print(paste('alldone with',pemsfiles.dir))
}else{
  print(paste(remaining, 'remaining after iterating at ',seconds,'seconds'))
}


dbDisconnect(con)

print('quit all done')
q(save = "default", status = 0)
