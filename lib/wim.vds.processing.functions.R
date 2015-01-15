library('Zelig')
dump.tsdf.to.DB.file <- function(vds.id,df,target.file,...){
  vds.lanes <- 1
  names.vds <- names(df)
  while(is.element(paste("nr",vds.lanes,sep=''),names.vds)){
    vds.lanes <- vds.lanes+1
  }
  n.idx <- vds.lane.numbers(vds.lanes,c("n"))
  o.idx <- vds.lane.numbers(vds.lanes,c("o"))

  ## create the df suitable for dumping to db
  dump <- data.frame(vds_id=vds.id,ts=df$ts)
  dump$vol <-  apply(df[,n.idx], 1, sum)
  dump$occ <-   apply(df[,o.idx], 1, sum)
  # fixme change this when I get speed
  dump$spd <- NA
  # fixme possible change the sd stuff when/if I read the documentation on handling multiples
  dump$sd_vol <- NA
  dump$sd_occ <- NA
  dump$sd_spd <- NA
  db.legal.names  <- make.db.names(con,names(dump),unique=TRUE,allow.keywords=FALSE)
  names(dump) <- db.legal.names
  ## fs write
  write.csv(dump,file=target.file,row.names = FALSE,...)
}

make.vds.wim.imputed.name <- function(wim,vds,year){
  paste('wim',wim,'vds',vds,'imputed',year,'RData',sep='.')
}
save.imputed.combined <- function(aout,path,wim,vds,year){
  fname <- make.vds.wim.imputed.name(wim,vds,year)
  print(fname)
  result <- try(save(aout,file=paste(path,fname,sep='/'),compress='xz'))
  ## I keep getting errors saving, that bomb out my program.
  if(class(result) == "try-error"){
    print ("\n Error saving file with XZ compression, trying gzip instead \n")
    save(aout,file=paste(path,fname,sep='/'),compress='gzip')
  }
}
check.imputed.combined<- function(path,wim,vds,year){
  result = FALSE
  for (vdsi in 1:length(vds)){
    aout <- 'failed'
    fname <- paste('wim',wim,'vds',vds[vdsi],'imputed',year,'RData',sep='.')
    done.file <- dir(path, pattern=fname,
                   full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
    if(length(done.file)>0){
      result = TRUE;
      return(result);
    }
  }
  result
}

load.imputed.combined <- function(path,wim,vds,year){

  ## vds could be an array, or it might not, collapse handles that
  alldata <- data.frame()
  for (vdsi in 1:length(vds)){
    aout <- 'failed'
    fname <- paste('wim',wim,'vds',vds[vdsi],'imputed',year,'RData',sep='.')
    done.file <- dir(path, pattern=fname,
                   full.names=TRUE, ignore.case=TRUE,recurs=TRUE)
    if(length(done.file)>0){
      print(paste('loading imputed combined file',done.file[1]))
      load.result <-  load(file=done.file[1])

      ## need to merge these here
      aout <- add.detector.id(aout,vds[vdsi],'vds_id')
      problem.vars <- grep( pattern="_all_veh_speed_",x=names(aout),perl=TRUE,value=TRUE)
      ## can't see how to do this all at once
      for(var in problem.vars){
        negative.vals <- aout[,var]<0 & ! is.na(aout[,var])
        aout[negative.vals,var] <- 0
      }
      if(length(alldata)==0){
        alldata <-  aout
      }else{
        alldata <- rbind( alldata, aout )
      }

    }
  }
  alldata
}

load.imputed.combined.prefetch <- function(path,pattern='wim.*reduced.*vds.*imputed.*RData',recursive=FALSE) {

  ## this version creates an array of the directory to check each time
  ## if the directory files change, this isn't appropriate, but
  ## otherwise it should be faster than the above.  Call with a
  ## pattern and a path, and it will load up the file names of all
  ## files in the path (recursive is an option) matching the pattern


  existing.files <- dir(path, pattern=pattern, full.names=TRUE, ignore.case=TRUE, recurs=recursive)

  ## return a function that can be called that searches from this list

  callback <- function(vds,year){

    ## vds could be an array, or it might not, collapse handles that
    alldata <- list()
    for (vdsi in 1:length(vds)){
      aout <- 'failed'
      vds.pattern <- paste( vds[vdsi],'.*',year,sep='')
      fname <- grep(vds.pattern,existing.files,perl=TRUE,value=TRUE)
      if(length(fname)>0){
        for(file in fname){
          print(paste('loading imputed combined file',file))
          load.result <-  load(file=file)
          if(!is.null(aout)){
            ## need to merge these here
            aout <- add.detector.id(aout,vds[vdsi],'vds_id')
            problem.vars <- grep( pattern="_all_veh_speed_",x=names(aout),perl=TRUE,value=TRUE)
            ## can't see how to do this all at once
            for(var in problem.vars){
              negative.vals <- aout[,var]<0 & ! is.na(aout[,var])
              aout[negative.vals,var] <- 0
            }
            alldata[[file]] <- aout
          }
        }
      }
    }
    alldata
  }
  return (callback)
}
