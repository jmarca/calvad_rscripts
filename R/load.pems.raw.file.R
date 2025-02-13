
##' load the raw file from PeMS
##'
##' Will scan the data and load up, then return the result of scanning
##'
##' @title load.raw.file
##' @param file the file to scan
##' @param skip the number of lines to skip at the beginning, in case there was a problem on the prior run.  passed to scan
##' @param nlines the number of lines to scan passed to scan
##' @return the result of scanning the file
##' @author James E. Marca
load.raw.file <- function (file="",skip=0,nlines=0){
  whatlist <- list(ts="",vds_id=0,
                   n1=0,o1=0,s1=0,
                   n2=0,o2=0,s2=0,
                   n3=0,o3=0,s3=0,
                   n4=0,o4=0,s4=0,
                   n5=0,o5=0,s5=0,
                   n6=0,o6=0,s6=0,
                   n7=0,o7=0,s7=0,
                   n8=0,o8=0,s8=0)
  testScan <-
    scan(file, what=whatlist, skip=skip,nlines=nlines,multi.line=FALSE,flush=TRUE,sep=',')
  testScan
}
##' load a given file, whether by scanning the raw file, or by loading
##' an existing RData file
##'
##' A side effect is that if the raw data file is scanned, then an
##' RData file will be saved to the file system to speed things up
##' next time
##'
##' @title load.file
##' @param f the full file name for the raw text.  optional if there
##'     is an RData file, but if there isn't an RData file you need this
##' @param fname the business end of the name.  Typically the part
##'     that identifies the device id and the year.  It will be passed
##'     to the dir() function as part of the pattern to match when
##'     looking for the RData file.
##' @param year the year of data.  Used when searching for RData file
##' @param path the path to the root of the thing.
##' @return a data frame with the data in it
##' @export
##' @author James E. Marca
##'
load.file <- function(f,fname,year,path){
  ## is there a df available?
  df <- data.frame()
  target.file =paste(fname,'.df.*',year,'.RData',sep='')
  isa.df <- dir(path, pattern=target.file,full.names=TRUE, ignore.case=TRUE,recursive=TRUE,all.files=TRUE)
  need.to.save <- FALSE
  if(length(isa.df)>0){
    print (paste('loading', isa.df[1]))
    load.result <-  load(file=isa.df[1])
  }else{
      if(!missing(f)){
          print (paste('scanning', f))
          fileScan <- load.raw.file(f)

          ## pre-process the vds data
          ts <- as.POSIXct(strptime(fileScan$ts,"%m/%d/%Y %H:%M:%S",tz='GMT'))
          fileScan$ts <- ts
          df <- trim.empty.lanes(fileScan)
          if(dim(df)[2]>2                    ## sometimes df is totally NA
             & is.element("n1",names(df))
             & is.element("o1",names(df))    ## sometimes get random interior lanes
             ){
              df <- recode.lanes(df[,-1]) ## drop time for recode lanes
              ## put back time
              df$ts <- ts
          }
          keep <- ! is.na(df$ts)
          df <- df[keep,]
          savepath <- get.save.path(f)
          save(df,
               file=paste(savepath,'/',fname,'.df.',year,'.RData',sep=''),
               compress='xz')
      }
  }
  df
}
##' The raw data often has empty lanes.  This will get rid of them.
##'
##' @title trim.empty.lanes
##' @param testScan the scanned data file
##' @return a data frame without lanes.
##' @author James E. Marca
trim.empty.lanes <- function(testScan){
  df <- data.frame(testScan)

  ## bug bit me in the behind.  If the first row has missing values but a non-missing time,
  ## then this will fail
  # pattern <- ! is.na(df[1,])
  ## so going with mean, which chokes on na's

  ## bleh.  Another bug, a little more difficult.  sometimes speed in
  ## one lane seems to be out.  so make sure if you have x lanes of n
  ## and o, you also have x lanes of speed or none at all

  ## old way: pattern <- ! is.nan(mean(df,na.rm=TRUE))
  pattern <- rep(TRUE,dim(df)[2])
  names(pattern) <- colnames(df)

  pattern[1]=FALSE ## keep time in the end, but ignore for now
  pattern[2]=FALSE ## don't need the vdsid for every record
  means <- colMeans(df[,pattern],na.rm=TRUE)
  pattern[pattern] <- ( ! is.nan(means) ) ## old way


  ## have seen cases with reasonable n, but nothing for o, and mostly NAs
  ## so cut off cases with more than 95% missing as junk
  pattern[pattern] <- (apply(df[pattern],2,pct.na) < 0.95)

  ## if any speeds are non null, then make sure speed lanes equals v and o lanes
  speed.names <- grepl( pattern="^s\\d",x=names(df),perl=TRUE)

  if(any( speed.names & pattern ) ){
    vol.names <- grepl( pattern="^n\\d",x=names(df),perl=TRUE)
    vol.lanes.with.data <- pattern[vol.names] & !is.na(pattern[vol.names])
    lanes <- length(vol.lanes.with.data[vol.lanes.with.data]  )
    while(lanes>0){
      pattern[paste('s',lanes,sep='')] <- TRUE
      lanes <- lanes-1
    }
  }
  ## keep time
  pattern[1] <- TRUE
  df[,pattern]
}
##' Determine the percent of entries that have NA values
##'
##' @title pct.na
##' @param v the vector to examine
##' @return the percentage the entries in the vector that are NA.
##' @author James E. Marca
pct.na <- function(v){
  sum(is.na(v))/length(v)
}

##' Get the canonical lane numbering system for a given number of lanes
##'
##' ##' For example, if you have count and occupancy, you'd pass in
##' c('n','o')
##' @return a vector consisting of the correct naming for the
##' variables.  For example, if you have one lane and pass in
##' c('n','o'), you will get back the vector c("nr1","or1").  If you
##' have three lanes and still just volume and occupancy c('n','o')
##' then you will get back c("nl1", "ol1","nr2", "or2","nr1", "or1")
##'
##' Note that this enforces my special naming scheme, in which a site
##' always has a right lane, and then a left lane, and then all other
##' lanes are coded from the right as r2, r3, etc.  So a 5 lanes site
##' will have, from left to right, l1, r4, r3, r2, r1.
##'
##' @title vds.lane.numbers
##' @param lanes the number of lanes in the desired data set
##' @param raw.data a vector of the values recorded for each lane.
##'
##' @author James E. Marca
vds.lane.numbers <- function(lanes,raw.data){

  Y <- paste(raw.data,"l1", sep="")
  YL <- paste(raw.data,"l1", sep="")
  YR <- paste(raw.data,"r1", sep="")
  if(lanes>2){
    ## interior lanes
    YM <- paste(raw.data,
                "r",
                rep(sort(rep(c((lanes-1):2),length(raw.data)),decreasing=TRUE)),
                sep="")
    Y <- c(YL,YM,YR)
  }else{
    ## no interior lanes, could also be a ramp
    if(lanes==1){
      Y <- c(YR)
    }else{
      ## lanes == 2
      Y <- c(YL,YR)
    }
  }

  Y
}

recode.lanes <- function(df){
                                        # run this only after you've
                                        # run trim empty lanes

## This is broken if the lanes are screwy after trim empty lanes
## example:
## > summary(df)
##        n1               n2               n5                  o5                 n6               o6
##  Min.   :     0   Min.   :     0   Min.   :0.000e+00   Min.   :0.000e+00  Min.   :     0   Min.   :     0
##  1st Qu.:     0   1st Qu.:     0   1st Qu.:0.000e+00   1st Qu.:0.000e+00  1st Qu.:     0   1st Qu.:     0
##  Median :     0   Median :     0   Median :0.000e+00   Median :0.000e+00  Median :     0   Median :     0
##  Mean   :     0   Mean   :     0   Mean   :6.646e-02   Mean   :9.154e-04  Mean   :     0   Mean   :     0
##  3rd Qu.:     0   3rd Qu.:     0   3rd Qu.:0.000e+00   3rd Qu.:0.000e+00  3rd Qu.:     0   3rd Qu.:     0
##  Max.   :     0   Max.   :     0   Max.   :1.800e+01   Max.   :9.044e-01  Max.   :     0   Max.   :     0
##  NA's   :521354   NA's   :521354   NA's   :2.013e+05   NA's   :2.013e+05  NA's   :201302   NA's   :201302

## here I have n1, n2, but not n3, n4, and not o[1..4], so recode thinks there are 2 lanes, and recodes the names on teh wrong variables.



  ##
  ## recode to be right lane (r1), right lane but one (r2), r3, ... and then
  ## left lane (l1)
  ##

  names.data <- names(df)

  ## testing:
  ## names.data <-c('n1', 'n2',  'n5', 'o5', 'n6', 'o6')
  ## eventually recode this to use grep.
  YR <- NULL
  for (site.lanes in 1:8){
     r.d <- NULL
     if( is.element(paste("n",site.lanes,sep=''),names.data)){
          r.d <- c('n')
     }
     if( is.element(paste("o",site.lanes,sep=''),names.data)){
          r.d <- c(r.d,'o')
     }
     if( is.element(paste("s",site.lanes,sep=''),names.data)){
          r.d <- c(r.d,'s')
     }
     if(!is.null(r.d)){
         if(site.lanes == 1){
             YR <- c(YR, paste(r.d,'l1',sep=""))
         }else{
             YR <- c(YR, paste(r.d,paste('r',site.lanes-1,sep=""),sep=""))
         }
     }
  }

  names(df) <- YR
  df
}
