get.save.path <- function(f){
    file.names <- strsplit(f,split="/")[[1]]
    savepath <- paste(file.names[-(length(file.names))],collapse='/')
}

unzoo.incantation <- function(df.z){
  ts.ts <- unclass(time(df.z))+ISOdatetime(1970,1,1,0,0,0,tz='UTC')
  keep.columns <-  grep( pattern="(^ts|^day|^tod|^obs_count)",x=names(df.z),perl=TRUE,value=TRUE,invert=TRUE)
  df.m <- data.frame(coredata(df.z[,keep.columns]))
  df.m$ts <- ts.ts
  ts.lt <- as.POSIXlt(df.m$ts)
  df.m$tod   <- ts.lt$hour + (ts.lt$min/60)
  df.m$day   <- ts.lt$wday
  df.m
}


transpose.lanes.to.rows <- function(df){

  varnames <- names(df)
  lane.pattern = '(r\\d+$|l1)$';
  not.lanes <- grep(pattern=lane.pattern,x=varnames,perl=TRUE,value=TRUE,inv=TRUE)
  lanes <- regexec(lane.pattern,varnames)
  lanes <- unique(unlist( regmatches(varnames, lanes) ))
  recode <- data.frame()
  for(lane in lanes){
    lane.columns <- grep(pattern=paste(lane,'$',sep=''),x=varnames,perl=TRUE,value=TRUE)
    if(!length(lane.columns)>0){
      next
    }
    keepvars <- c(not.lanes,lane.columns)
    subset <- data.frame(df[,keepvars])
    names(subset) <- gsub(paste('_?',lane,'$',sep=''),'',keepvars,perl=TRUE)
    subset$lane <- lane

    if(length(recode)==0){
      recode <- subset
    }else{
      ## make sure names match up
      missing.recode <- setdiff(names(recode),names(subset))
      missing.subset <- setdiff(names(subset),names(recode))

      if(length(missing.recode)>0){
        subset[,missing.recode] <- NA
      }
      if(length(missing.subset)>0){
        recode[,missing.subset] <- NA
      }
      recode <- rbind(recode,subset)
    }
  }
  recode
}

district.from.vdsid <- function(vdsid){
  district <- as.integer(sub( "\\d{5}$","",x=vdsid))
  if(district < 10){
    district <- paste('d0',district,sep='')
  }else{
    district <- paste('d',district,sep='')
  }
  ## setup.district.replication(district,year)
  district
}
