## library('Amelia')
## everything options are:
## ,count.pattern = "^(not|empty|heavyheavy|nl|nr)"
## ,mean.pattern="(^ol|^or|_weight|_axle|_length|_speed)"
## ,mean.exclude.pattern="^(mean)"
## ){



## old version
                            ## ,count.pattern = "(heavyheavy|^nl|^nr\\d|count_all_veh_speed)"
                            ## ,mean.pattern="(^ol|^or\\d|_weight|_axle|_length|_speed||wgt_spd_all_veh_speed)"
                            ## ,mean.exclude.pattern="^(mean|mt_|tr_)"

fill.truck.gaps <- function(df
                            ,count.pattern = "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
                            ,mean.pattern="(^ol|^or\\d)"
                            ,exclude.pattern="^(mean|mt_|tr_)"
                            ,maxiter=200
                            ){

  ## truncate negative values to zero
  negatives <-  !( df>=0 | is.na(df) )
  df[negatives] <- 0

  ## sort out upper limits
  occ.pattern <- "(^ol1$|^or\\d$)"
  ic.names <- names(df)
  ic.names <- grep( pattern=exclude.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=TRUE)
  sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
  ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)
  occ.vars <-  grep( pattern=occ.pattern,x=mean.vars ,perl=TRUE,value=TRUE)
  M <- 10000000000  #arbitrary bignum
  ic.names <- names(df)
  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]
  max.vals <- apply( df[,count.vars], 2, max ,na.rm=TRUE)
  pos.bds <- cbind(pos.count,0,1.10*max.vals)
  ## limit the mean vars less, but exclude occ vars
  if(length(setdiff(mean.vars,occ.vars))>0){
    pos.count <- (1:length(ic.names))[is.element(ic.names, setdiff(mean.vars,occ.vars))]
    pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))
  }
  ## now limit occupancy to (0,1)
  pos.count <- (1:length(ic.names))[is.element(ic.names, occ.vars)]
  pos.bds <- rbind(pos.bds,cbind(pos.count,0,1))

  print("bounds:")
  print(pos.bds)

  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))
  print(paste("count vars:",paste(count.vars,collapse=' ')))
  print(paste("mean vars:", paste(mean.vars,collapse=' ')))
  print(paste("excluded:",  paste(exclude.as.id.vars,collapse=' ')))
  ## this version is thorough, but does not run fast enough for the moment
  ## df.amelia <-
  ##   amelia(df,idvars=exclude.as.id.vars,
  ##          ts="tod",
  ##          splinetime=6,
  ##          lags =count.vars,leads=count.vars,
  ##          sqrts=count.vars,
  ##          cs="day",
  ##          intercs=TRUE,
  ##          emburn=c(2,maxiter),
  ##          bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df))
  ## this one works
  ## df.amelia <-
  ##   amelia(df,idvars=exclude.as.id.vars,
  ##          ts="tod",
  ##          splinetime=3,
  ##          ## lags =count.vars,leads=count.vars,
  ##          sqrts=count.vars,
  ##          cs="day",
  ##          intercs=TRUE,
  ##          emburn=c(2,maxiter),
  ##          bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df)
  ##          )
  ## this one is going for speed
  df$toddow <- 24 * df$day + df$tod

  exclude.as.id.vars <- c('tod','day',exclude.as.id.vars)
    df.amelia <-
    amelia(df,idvars=exclude.as.id.vars,
           ts="toddow",
           splinetime=6,
           #lags =count.vars,
           #leads=count.vars,
           sqrts=count.vars,
           #cs="day",
           #intercs=TRUE,
           emburn=c(2,maxiter),
           bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df)
           ##,m=1 ## desperate measures!  set to limit the imputations
           )
  df.amelia
}

## old version
                            ## ,count.pattern = "(heavyheavy|^nl|^nr1|^nr2)"
                            ## ,mean.pattern="(^ol|^or1|^or2|_weight|_axle|_length|_speed)"
                            ## ,mean.exclude.pattern="^(mean|mt_|tr_)"

names.munging <- function(names.df
                            ,count.pattern = "(heavyheavy|^nl|^nr\\d|_weight|_axle|_length|_speed|_all_veh_speed)"
                            ,mean.pattern="(^ol|^or\\d)"
                            ,exclude.pattern="^(mean|mt_|tr_)"
                            ,df=data.frame()
                          ){

  ic.names <- names.df
  occ.pattern <- "(^ol1$|^or\\d$)"
  ic.names <- grep( pattern=exclude.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=TRUE)
  sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
  ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
  ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)
  mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)
  occ.vars <-  grep( pattern=occ.pattern,x=mean.vars ,perl=TRUE,value=TRUE)

  M <- 10000000000  #arbitrary bignum
  ic.names <- names.df

  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]

  pos.bds <- c()
  if(length(df)>0){
    max.vals <- apply( df[,count.vars], 2, max ,na.rm=TRUE)
    pos.bds <- cbind(pos.count,0,1.10*max.vals)
    ## limit the mean vars less, but exclude occ vars
    if(length(setdiff(mean.vars,occ.vars))>0){
      pos.count <- (1:length(ic.names))[is.element(ic.names, setdiff(mean.vars,occ.vars))]
      pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))
    }
    ## now limit occupancy to (0,1)
    pos.count <- (1:length(ic.names))[is.element(ic.names, occ.vars)]
    pos.bds <- rbind(pos.bds,cbind(pos.count,0,1))
  }

  pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]
  exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))

  ##  print(paste("count vars:",paste(count.vars,collapse=' ')))
  ##  print(paste("mean vars:", paste(mean.vars,collapse=' ')))
  list(exclude.as.id.vars=exclude.as.id.vars,
       pos.count=pos.count,
       mean.vars=mean.vars,
       count.vars=count.vars,
       pos.bds=pos.bds
       )
}


## the commented version


## fill.truck.gaps <- function(df
##                             ,count.pattern = "^(truck|heavyheavy|nl|nr)"
##                             ,truck.count.pattern = "^(truck|heavyheavy)"
##                             ,mean.pattern="(^ol|^or|_weight|_axle|_len|_speed)"
##                             ,mean.exclude.pattern="^(mean)"
##                             ){


##   ## run amelia on the combined vds and wim data.  the vds data has
##   ## already been imputed on itself, so should have no gaps.  ditto
##   ## for the wim data

##   ## imputed combined is the merged set of aggregated, imputed,  wim data merged with vds data, and also merged with a non-paired vds data

##   ##
##   ## I had the idea that first I want to just impute counts, and then
##   ## later I want to impute variables that depend on the counts, like
##   ## weight, speed, and axles
##   ##

##   ic.names <- names(df)
##   ## keep standard deviation data from the data
##   sd.vars <-   grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE)
##   ic.names <-  grep( pattern="sd(\\.|_)[r|l]\\d+$",x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)

##   count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)
##   ic.names<- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE,invert=TRUE)


##   ## sort out the "mean variables" that are really sums at this point
##   mean.vars <- grep( pattern=mean.pattern,x=ic.names,perl=TRUE,value=TRUE)

##   ## exclude some too so things don't get non-invertible and highly correlated
##   mean.vars <- grep( pattern=mean.exclude.pattern,x=mean.vars ,perl=TRUE,value=TRUE,invert=TRUE)


##   M <- 10000  #arbitrary bignum
##   # reset ic.names now
##   ic.names <- names(df)

##   truck.related.vs <- grep( pattern=truck.count.pattern,x=count.vars,perl=TRUE,value=TRUE)
##   not.truck.vs <- grep( pattern='^truck_',x=truck.related.vs,perl=TRUE,value=TRUE,invert=TRUE)
##   ## reset each of these non-truck count values to a fraction of the
##   ## truck count
##   truck.vs <- grep( pattern='^truck_',x=truck.related.vs,perl=TRUE,value=TRUE)

##   ## what I am doing here is getting rid of truck variables.  each
##   ## truck related category gets split into two categories, it, and
##   ## not it. so truck counts can always be restored by adding it and
##   ## not it. And I am using new.cnt.vs to track that migration without
##   ## mucking about with more regexes than I'm already (ab)using.
##   new.cnt.vs <- setdiff(count.vars,truck.related.vs)
##   new.cnt.vs <- c(new.cnt.vs,not.truck.vs)

##   lane.types <- strsplit(truck.vs,"^truck_")
##   for(l.type in lane.types){
##     ## match corresponding lanes in not.truck.vars
##     this.lane.vars <- grep(pattern=l.type[2],x=not.truck.vs,perl=TRUE,value=TRUE)
##     this.truck.var <- grep(pattern=l.type[2],x=truck.vs,perl=TRUE,value=TRUE)
##     for(vvar in this.lane.vars){
##       new.var <- paste('not',vvar,sep='.')
##       df[,new.var] <- df[,this.truck.var] - df[,vvar]
##       new.cnt.vs <- c(new.cnt.vs,new.var)
##     }
##   }

##   ## now build up all the final variables for the amelia call

##   ## first get all the names in the data frame
##   ic.names <- names(df)

##   ##make sure there are not dupes

##   ## the count variables
##   new.cnt.vs <- intersect(new.cnt.vs,ic.names)

##   ## generate the position of each count variable
##   pos.count <- (1:length(ic.names))[is.element(ic.names, c(new.cnt.vs))]

##   ## for the count variables,
##   ## make a max allowed value of 110% of observed values and a min value of 0
##   max.val <- max(df[,new.cnt.vs],na.rm=TRUE)
##   pos.bds <- cbind(pos.count,0,1.10*max.val)

##   ## for other variables, make a looser bound of zero to M
##   ## this fairly loose bound
##   ## generate the position of each count variable
##   pos.count <- (1:length(ic.names))[is.element(ic.names, mean.vars)]
##   pos.bds <- rbind(pos.bds,cbind(pos.count,0,M))


##   ## Impute some but not all of the variables.  I want to impute the
##   ## mean vars, but not nasty vars, sd vars, the truck counts, and any
##   ## other variables that weren't selected by the count var passed
##   ## pattern.

##   ## so instead of listing each, figure out which aren't

##   exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,new.cnt.vs,'tod','day'))

##   df.amelia <-
##     amelia(df,idvars=exclude.as.id.vars,
##            ts="tod",
##            ##splinetime=3,
##            ##lags =new.cnt.vs,leads=new.cnt.vs,
##            sqrts=new.cnt.vs,
##            cs="day",
##            # intercs=TRUE,
##            emburn=c(2,maxiter),
##            bounds = pos.bds, max.resample=10,empri = 0.05 *nrow(df))

##   df.amelia

## }
