### you need to have an existing value for con, a dbConnect object
### before the db query code here will work


## to get from the db directly, without aggregating
## pretty much use this for now
load.wim.data.straight <- function(wim.site,year){
  start.wim.time <-  paste(year,"-01-01",sep='')
  end.wim.time <-   paste(year+1,"-01-01",sep='')
  get.wim.site.2(wim.site,start.time=start.wim.time,end.time=end.wim.time)
}

get.list.wim.sites <-  function(){
  #   wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations join wim_district on (wim_id=site_no) where district_id in (7,8,11,12) and lanes>1 and wim_type != 'PrePass'" )
  #   wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations join wim_district on (wim_id=site_no) where district_id in (1,2,3,4,5,6,9,10) and lanes>1 and wim_type != 'PrePass'" )
  wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations where lanes>1 and wim_type != 'PrePass'" )
  print(wim.query)
  rs <- dbSendQuery(con,wim.query)
  df.wim <- fetch(rs,n=-1)
  df.wim
}


get.wim.sites <- function(end.time='2011-01-01'){
  wim.query <- "select distinct site_no as wim_id from wim_lane_dir order by site_no"
  print(wim.query)
  rs <- dbSendQuery(con,wim.query)
  df.wim <- fetch(rs,n=-1)
  df.wim
}

## the logic here is to get vds sites, then go back with a second
## query to get any and all WIM sites that are neigbors
get.list.neighbor.vds.sites <- function(){

  ##  db.query <- paste("select distinct c.vds_id,c.direction from (select a.* from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) where wim_id is null) c order by c.vds_id")
  ## select a.* from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) where wim_id is null;

  ## modify to *not* avoid the paired sites??

  ## drop direction, Task #724
  ## ignore non-ML sites to speed up rejections
  # db.query <- paste("select distinct c.vds_id from imputed.vds_wim_neighbors  c join  newtbmap.tvd a on (a.id = c.vds_id)  where a.vdstype = 'ML' order by c.vds_id")

  # get rid of that because tvd doesn't have all of the site for some reason.
  # also most of the sites in neighbors are mainline anyway these days.
  db.query <- paste("select distinct c.vds_id from imputed.vds_wim_neighbors  c  order by c.vds_id")

  print(db.query)
  rs <- dbSendQuery(con,db.query)
  df.q <- fetch(rs,n=-1)
  df.q
}

## And now the second half of the above query.  Pass the VDS id and
## the direction to get any neighboring WIM sites to use
get.list.neighbor.wim.sites <- function(vds.id){

##  db.query <- paste("select distinct c.site_no as wim_id,c.dist as distance from (select a.* from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) where wim_id is null and a.vds_id =",vds.id,' and a.direction=\'',relation.direction,'\'',  ") c",sep='')
## that above one prevents imputing paired sites.  Not sure that is good.  trying the following

## select direction, Task #724
##db.query <- paste("select distinct c.site_no as wim_id,c.dist as distance,direction from (select a.* from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) where a.vds_id =",vds.id, ") c",sep='')

  ## select a.* from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) where wim_id is null;

  # add lane count too
  withstatement <- "with maxlanes as (select site_no, newctmlmap.canonical_direction(direction) as direction, max(lane_no) as lanes from wim_lane_dir group by site_no, direction order by site_no, direction) "
  selectstatement <- " select distinct c.site_no as wim_id, c.dist as distance, direction, lanes from "
  subselect <- " (select a.*,ml.lanes from imputed.vds_wim_neighbors a left outer join imputed.vds_wim_pairs b on (a.vds_id=b.vds_id and a.site_no=b.wim_id) join maxlanes ml on (ml.site_no=a.site_no and newctmlmap.canonical_direction(a.direction)=ml.direction) "
  db.query <- paste(withstatement, selectstatement,subselect," where a.vds_id =",vds.id, ") c order by lanes desc",sep='')

  print(db.query)
  rs <- dbSendQuery(con,db.query)
  df.q <- fetch(rs,n=-1)
  df.q
}

get.list.district.neighbor.wim.sites <- function(vdsid){
  with.part <- "with maxlanes as (select site_no, newctmlmap.canonical_direction(direction) as direction, max(lane_no) as lanes from wim_lane_dir group by site_no, direction order by site_no, direction) "
  select.part <- "select distinct w.site_no as wim_id,ST_Distance_Sphere(v.geom,w.geom) as distance,newctmlmap.canonical_direction(w.direction) as direction,ml.lanes as lanes"
  from.part <- paste("from newtbmap.tvd v ",
                     "join wim_district wd on (wd.district_id=v.district)",
                     "join newctmlmap.wim_view w on (wd.wim_id=w.site_no)",
                     "join imputed.vds_wim_pairs wp on (w.site_no = wp.wim_id and newctmlmap.canonical_direction(w.direction) = newctmlmap.canonical_direction(wp.direction))",
                     "join maxlanes ml on (w.site_no = ml.site_no and newctmlmap.canonical_direction(w.direction) = ml.direction)")
  where.part <- paste("where v.id=",vdsid,"order by lanes desc")
  query <- paste(with.part,select.part,from.part,where.part)
  print(query)
  rs <- dbSendQuery(con,query)
  df.wim <- fetch(rs,n=-1)
  df.wim
}



get.list.closest.wim.pairs <- function(){

  wim.query <- paste("select vds_id,wim_id,direction from imputed.vds_wim_pairs")

  print(wim.query)
  rs <- dbSendQuery(con,wim.query)
  df.q <- fetch(rs,n=-1)
  df.q
}

get.wim.site.speed <- function(wim.siteno,start.time,end.time){
  query <- paste(
                 "select extract(epoch from ts) as ts, b.lane_no as lane, veh_speed, veh_count, direction from  wim.summaries_5min_speed a join wim_lane_dir b on (b.site_no=a.site_no and  b.wim_lane_no=a.wim_lane_no) where  ts>='",start.time,"' and ts < '",end.time, "' and a.site_no=",wim.siteno)

  print(query)

  rs <- dbSendQuery(con,query)
  df <- fetch(rs,n=-1)
  df$ts <- as.POSIXct(unclass(df$ts) + ISOdatetime(1970,1,1,0,0,0, tz="UTC"), tz="UTC" )
  dbClearResult(rs)
  rm('rs')
  df
}



get.wim.site.2 <- function(wim.siteno,start.time='2007-01-01',end.time='2008-01-01'){
  ## get the WIM data note that I am joining with wim_lane_dir because
  ## the wim lanes are numbered erratically.  I need to get the lane
  ## correct for Caltrans lane numbering scheme.  You'd think that
  ## Caltrans WIM would number according to Caltrans guidelines, but
  ## they dont!  The Feds say to number lanes for truck counting from the right.

  ## on processing the PAT data, I see that I should be aware that
  ## sometimes axle data is missing for PAT data.  that is why I added
  ## gross_weight to calc of total_axle_weight, and ditto veh_len to
  ## total_axle_spacing

  wim.query <- paste(
                     "select a.site_no , ws.weight_status,ws.class_status, EXTRACT(EPOCH FROM a.ts) as ts , b.lane_no as lane , veh_no , veh_class , gross_weight , veh_len , speed , violation_code ,"
                     ,"coalesce( (axle_9_rt_weight + axle_9_lt_weight +axle_8_rt_weight + axle_8_lt_weight +axle_7_rt_weight + axle_7_lt_weight +axle_6_rt_weight + axle_6_lt_weight +axle_5_rt_weight + axle_5_lt_weight +axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_8_rt_weight + axle_8_lt_weight +axle_7_rt_weight + axle_7_lt_weight +axle_6_rt_weight + axle_6_lt_weight +axle_5_rt_weight + axle_5_lt_weight +axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_7_rt_weight + axle_7_lt_weight +axle_6_rt_weight + axle_6_lt_weight +axle_5_rt_weight + axle_5_lt_weight +axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_6_rt_weight + axle_6_lt_weight +axle_5_rt_weight + axle_5_lt_weight +axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_5_rt_weight + axle_5_lt_weight +axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_4_rt_weight + axle_4_lt_weight +axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_3_rt_weight + axle_3_lt_weight +axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ),"
                     ,"(axle_2_rt_weight + axle_2_lt_weight +axle_1_rt_weight + axle_1_lt_weight ), gross_weight )as total_axle_weight,"
                     ,"coalesce("
                     ,"(axle_8_9_spacing+axle_7_8_spacing+axle_6_7_spacing+axle_5_6_spacing+axle_4_5_spacing+axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_7_8_spacing+axle_6_7_spacing+axle_5_6_spacing+axle_4_5_spacing+axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_6_7_spacing+axle_5_6_spacing+axle_4_5_spacing+axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_5_6_spacing+axle_4_5_spacing+axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_4_5_spacing+axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_3_4_spacing+axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_2_3_spacing+axle_1_2_spacing),"
                     ,"(axle_1_2_spacing), veh_len ) as total_axle_spacing,"
                     ,"CASE WHEN axle_8_9_spacing IS NOT NULL THEN 9"
                     ," WHEN axle_7_8_spacing IS NOT NULL THEN 8"
                     ," WHEN axle_6_7_spacing IS NOT NULL THEN 7"
                     ," WHEN axle_5_6_spacing IS NOT NULL THEN 6"
                     ," WHEN axle_4_5_spacing IS NOT NULL THEN 5"
                     ," WHEN axle_3_4_spacing IS NOT NULL THEN 4"
                     ," WHEN axle_2_3_spacing IS NOT NULL THEN 3"
                     ," WHEN axle_1_2_spacing IS NOT NULL THEN 2"
                     ," ELSE NULL END as total_axles,"
                     ## ,"    (axle_8_9_spacing IS NOT NULL AND axle_9_rt_weight + axle_9_lt_weight > 18 )"
                     ## ," OR (axle_7_8_spacing IS NOT NULL AND axle_8_rt_weight + axle_8_lt_weight > 18 )"
                     ## ," OR (axle_6_7_spacing IS NOT NULL AND axle_7_rt_weight + axle_7_lt_weight > 18 )"
                     ## ," OR (axle_5_6_spacing IS NOT NULL AND axle_6_rt_weight + axle_6_lt_weight > 18 )"
                     ## ," OR (axle_4_5_spacing IS NOT NULL AND axle_5_rt_weight + axle_5_lt_weight > 18 )"
                     ## ," OR (axle_3_4_spacing IS NOT NULL AND axle_4_rt_weight + axle_4_lt_weight > 18 )"
                     ## ," OR (axle_2_3_spacing IS NOT NULL AND axle_3_rt_weight + axle_3_lt_weight > 18 )"
                     ## ," OR axle_2_rt_weight + axle_2_lt_weight > 18"
                     ## ," OR axle_1_rt_weight + axle_1_lt_weight > 18 as overweight,"
                     ," direction "
                     ,"from wim_data a"
                     ,"join wim_lane_dir b on (b.site_no=a.site_no and b.wim_lane_no=a.lane)"
                     ,"join wim_status ws on (b.site_no=ws.site_no and date_trunc('month',a.ts) = ws.ts)"
                     ,"where"
                     ,"veh_class>3"
                     ,"and (ws.class_status in ('M','G') or ws.weight_status in ('M','G'))"
                     ,"and a.ts>='",start.time,"' and a.ts < '",end.time,
                     "' and a.site_no=",wim.siteno)

  print(wim.query)

  rs <- dbSendQuery(con,wim.query)
  df <- fetch(rs,n=-1)
  df$ts <- as.POSIXct(unclass(df$ts) + ISOdatetime(1970,1,1,0,0,0, tz="UTC") ,tz='UTC')
  dbClearResult(rs)
  rm(rs)
  df
}


get.wim.status <- function(wim.site){
  ## it is important to ignore bad data
  wim.status.query <-
    paste("select site_no , ts , class_status , class_notes , weight_status , weight_notes from wim_status where ts>='2007-01-01' and ts < '2009-01-01' and site_no=",wim.site)

  rs <- dbSendQuery(con,wim.status.query)
  df.wim.status <- fetch(rs,n=-1)
  df.wim.status
}

## parameterize count variables here such that ,count.vars=c("truck","heavyheavy","overweight")
process.wim.2 <- function(df.wim){

  df.wim$total_axle_weight[df.wim$weight_status=='B'] <- NA
  df.wim$gross_weight[df.wim$weight_status=='B'] <- NA
  df.wim$veh_class[df.wim$class_status=='B'] <- NA

  ## now doing most of this in the db query
  df.wim$truck <- 1
  ## code up the ARB classification scheme
  df.wim$heavyheavy <- df.wim$gross_weight>=33 | df.wim$total_axles>3
  df.wim$heavyheavy[is.na(df.wim$total_axles) & is.na(df.wim$heavyheavy)] <- df.wim$gross_weight[is.na(df.wim$total_axles) & is.na(df.wim$heavyheavy)]>=33
  df.wim$heavyheavy[is.na(df.wim$gross_weight) & is.na(df.wim$heavyheavy)] <- df.wim$total_axles[is.na(df.wim$gross_weight) & is.na(df.wim$heavyheavy)]>3


  ## possibly.empty.axle <- (df.wim$total_axle_weight-df.wim$axle_1_rt_weight -df.wim$axle_1_lt_weight)/(df.wim$total_axles-1)<8

  ## skip that, as it would require more download from db, and it
  ## isn't supported by much more than eyeballing curves anyway.
  ## instead just do

  ## possibly.empty.axle <- df.wim$total_axle_weight/df.wim$total_axles < 8
  ## possibly.empty.weight <- df.wim$total_axle_weight < 30
  df.wim$empty <- df.wim$total_axle_weight/df.wim$total_axles < 8  & df.wim$total_axle_weight < 30
  df.wim
}


wim.lane.numbers <- function(lanes){

  Y <- "l1"
  YL <- "l1"
  YR <- "r1"
  if(lanes>2){
    ## interior lanes
    YM <- paste("r",
                (lanes-1):2,
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


wim.recode.lanes <- function(df){
                                        # run this only after you've
                                        # run trim empty lanes

  ##
  ## recode to be right lane (r1), right lane but one (r2), r3, ... and then
  ## left lane (l1)
  ##

  lanes <- max(df$lane)
  Y <- wim.lane.numbers(lanes)
  df$lane <- as.factor(Y[df$lane])
  df
}


## plotting and information

pretty.thistogram <- function(x,main="Histogram",xlab="",ylab=""){
  ## get rid of NA values
  x <- x[!is.na(x)]
  truehist( x, xlab=xlab,main=main )
                                        # calculate density estimation
  d <- density( x )

  abline( h = axTicks(2) , col = "gray", lwd = .5 )
                                        # adds a tick and translucent line to display the density estimate
  lines( d, lwd = 4, col = "#66666688" )
                                        # add a rug (also using translucency)
  rug( x, col = "#00000088" )
}

smooth.color.scatter <- function(x,main="Histogram",xlab="",ylab=""){
  smoothScatter(x, nrpoints=floor(0.0001*length(x[,1])),
                colramp=colorRampPalette( c("white","red")),
                transformation = function(x) {x^0.25},
                main=main,xlab=xlab,ylab=ylab  )

  ## a different color scheme:
  ##  Lab.palette <- colorRampPalette(c("white","blue", "orange", "red"), space = "Lab")
  ##  smoothScatter(x, colramp = Lab.palette)
}

wim.plot.gen <- function(file,wim.site,df.wim,width=850,height=850,...){

  png(file = file,
      width=width, height=height, bg="transparent",...)

  ## plots that might be useful
## > names(df.wim.300)
##  [1] "truck.r1"        "heavyheavy.r1"   "overweight.r1"   "over_weight.r1"
##  [5] "legal_weight.r1" "hh_weight.r1"    "hh_axles.r1"     "hh_len.r1"
##  [9] "hh_speed.r1"     "nh_weight.r1"    "nh_axles.r1"     "nh_len.r1"
## [13] "nh_speed.r1"     "truck.r2"        "heavyheavy.r2"   "overweight.r2"
## [17] "over_weight.r2"  "legal_weight.r2" "hh_weight.r2"    "hh_axles.r2"
## [21] "hh_len.r2"       "hh_speed.r2"     "nh_weight.r2"    "nh_axles.r2"
## [25] "nh_len.r2"       "nh_speed.r2"     "truck.r3"        "heavyheavy.r3"
## [29] "overweight.r3"   "over_weight.r3"  "legal_weight.r3" "hh_weight.r3"
## [33] "hh_axles.r3"     "hh_len.r3"       "hh_speed.r3"     "nh_weight.r3"
## [37] "nh_axles.r3"     "nh_len.r3"       "nh_speed.r3"

##  [1] "site_no"            "ts"                 "lane"
##  [4] "veh_no"             "veh_class"          "gross_weight"
##  [7] "veh_len"            "speed"              "violation_code"
## [10] "total_axle_weight"  "total_axle_spacing" "total_axles"
## [13] "overweight"         "truck"              "heavyheavy"
## [16] "over_weight"        "legal_weight"       "hh_weight"
## [19] "hh_axles"           "hh_len"             "hh_speed"
## [22] "nh_weight"          "nh_axles"           "nh_len"
## [25] "nh_speed"

  xyplot(df.wim$gross_weight ~ df.wim$total_axle_weight | factor(paste(df.wim$total_axles,"axle trucks")),
         panel=function(x,y){
           panel.smoothScatter(x,y,nbin=c(200,200))
         })

  xyplot(df.wim$gross_weight ~ df.wim$total_axle_spacing | factor(ifelse(df.wim$heavyheavy,"heavy heavy-duty","other")),
         panel=function(x,y){
           panel.smoothScatter(x,y,nbin=c(200,200))
         })
  pretty.thistogram(df.wim$gross_weight/df.wim$total_axles,main=paste("Histogram of WIM gross_weight/axles for all vehicles, at site",wim.site),xlab="gross_weight / total axles")
  pretty.thistogram(df.wim$gross_weight[df.wim$overweight]/df.wim$total_axles[df.wim$overweight],main=paste("Histogram of WIM gross_weight/axles for vehicles w/ at least one axle > 18kips , at site",wim.site),xlab="gross_weight / total axles")
  pretty.thistogram(df.wim$gross_weight[!df.wim$overweight]/df.wim$total_axles[!df.wim$overweight],main=paste("Histogram of WIM gross_weight/axles for vehicles with <= 18kips per axle, at site",wim.site),xlab="gross_weight / total axles")

  ##   bwplot(axle ~ weight | factor(paste(total_axles,"axle trucks")),data=axle.weight,xlab="axle loading, kips")


  pretty.thistogram(df.wim$veh_len,main=paste("Histogram of WIM vehicle length, at site",wim.site),xlab="measured vehicle length, feet")

  pretty.thistogram(df.wim$total_axle_spacing,main=paste("Histogram of WIM total inter-axle spacing, at site",wim.site),xlab="cumulative inter-axle spacing per vehicle, feet")

  ## xyplot(df.wim$veh_len ~ df.wim$total_axle_spacing, panel = function(x,y){
  ##   panel.smoothScatter(x,y,nbin=c(400,400))
  ## },main=paste("Length vs  inter-axle spacing, at site",wim.site),ylab="measured vehicle length, feet", xlab="total inter-axle spacing per vehicle, feet")

  dev.off()
}

aout.postimp.plots <- function(file,wim.site,aout,width=850,height=850,...){

  ts.lt <- as.POSIXlt(aout$imputations[[1]]$ts[1])
  year  <- ts.lt$year

  if(is.null(file)){

    file <- paste ('wim.imputed.',1900+year,'.',wim.site,'.%03d.png',sep='');

  }
  png(file = "wim.imputetests.2008.site.31.%03d.png",
      width=1000, height=1200, bg="transparent")

  ## the variables:
  ## names(df.truckamelia.c$imputations[[1]])
##  [1] "truck_r1"         "heavyheavy_r1"    "overweight_r1"    "over_weight_r1"
##  [5] "legal_weight_r1"  "hh_weight_r1"     "hh_axles_r1"      "hh_len_r1"
##  [9] "hh_speed_r1"      "nh_weight_r1"     "nh_axles_r1"      "nh_len_r1"
## [13] "nh_speed_r1"      "truck_r2"         "heavyheavy_r2"    "overweight_r2"
## [17] "over_weight_r2"   "legal_weight_r2"  "hh_weight_r2"     "hh_axles_r2"
## [21] "hh_len_r2"        "hh_speed_r2"      "nh_weight_r2"     "nh_axles_r2"
## [25] "nh_len_r2"        "nh_speed_r2"      "truck_r3"         "heavyheavy_r3"
## [29] "overweight_r3"    "over_weight_r3"   "legal_weight_r3"  "hh_weight_r3"
## [33] "hh_axles_r3"      "hh_len_r3"        "hh_speed_r3"      "nh_weight_r3"
## [37] "nh_axles_r3"      "nh_len_r3"        "nh_speed_r3"      "mean_vehspeed_l1"
## [41] "mean_vehspeed_r1" "mean_vehspeed_r2" "mean_vehspeed_r3" "ts"
## [45] "tod"              "day"
## >

  ## the api

  ##      tscsPlot(output, var, cs, draws = 100, conf = .90,
  ##            misscol = "red", obscol = "black", xlab, ylab, main,
  ##            pch, ylim, xlim, ...)
  week.days <- c('Sun','Mon','Tue','Wed','Thu','Fri','Sat')
  ## plots that might be useful
  day <- 0
  for(day in 0:6){
    ## tscsPlot(aout,'truck_r1',day,xlab='time of day',ylab='truck count, lane Right 1', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )
    ## tscsPlot(aout,'truck_r2',day,xlab='time of day',ylab='truck count, lane Right 2', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )
    ## tscsPlot(aout,'truck_r3',day,xlab='time of day',ylab='truck count, lane Right 3', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )

    ## bout <- aout
    ## cout <- aout
    ## for( imp in 1:length(bout$imputations)){
    ##   ## this.set <- bout$imputations[[imp]]

    ##   ## mask <- a.out$imputations[[i]]$count == 0
    ##   ## is.na(a.out$imputations[[i]]) <- mask

    ##   ## need to filter on non-zero counts some how
    ##   hh.count.filter <- bout$imputations[[imp]][,c("heavyheavy_r1","heavyheavy_r2","heavyheavy_r3")]==0
    ##   bout$imputations[[imp]][hh.count.filter[,1],c("hh_weight_r1","hh_axles_r1","hh_len_r1","hh_speed_r1" )] <- NA
    ##   bout$imputations[[imp]][hh.count.filter[,2],c("hh_weight_r2","hh_axles_r2","hh_len_r2","hh_speed_r2" )] <- NA
    ##   bout$imputations[[imp]][hh.count.filter[,3],c("hh_weight_r3","hh_axles_r3","hh_len_r3","hh_speed_r3" )] <- NA
    ##   hh.count.filter2 <- bout$imputations[[imp]][,"heavyheavy_r3"]==0
    ##   is.na(cout$imputations[[imp]]) <- hh.count.filter2
    ## }
    tscsPlot(aout,'truck_r1',day,xlab='time of day',ylab='trucks, lane Right 1', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'truck_r2',day,xlab='time of day',ylab='trucks, lane Right 2', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'truck_r3',day,xlab='time of day',ylab='trucks, lane Right 3', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'tr_weight_r1',day,xlab='time of day',ylab='sum weight of trucks, lane Right 1', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'tr_weight_r2',day,xlab='time of day',ylab='sum weight of trucks, lane Right 2', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'tr_weight_r3',day,xlab='time of day',ylab='sum weight of trucks, lane Right 3', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'heavyheavy_r1',day,xlab='time of day',ylab='hh trucks, lane Right 1', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'heavyheavy_r2',day,xlab='time of day',ylab='hh trucks, lane Right 2', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    tscsPlot(aout,'heavyheavy_r3',day,xlab='time of day',ylab='hh trucks, lane Right 3', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )

  }
  dev.off()
}

more.plot.scratch <- function(){

  zero.count <- bout$imputations[[1]]$heavyheavy_r3 == 0
  xyplot(
         bout$imputations[[1]]$hh_weight_r3[bout$missMatrix[,32] & ! zero.count ]
         ~
         bout$imputations[[1]]$tod[bout$missMatrix[,32] & ! zero.count]  |  bout$imputations[[1]]$day[bout$missMatrix[,32] & !zero.count ]
         )

  zero.count <- aout.all$imputations[[1]]$heavyheavy_r3 == 0
  xyplot(
         aout.all$imputations[[1]]$hh_weight_r3[aout.all$missMatrix[,32] & !zero.count ]
         ~
         aout.all$imputations[[1]]$tod[aout.all$missMatrix[,32] & !zero.count ] |  aout.all$imputations[[1]]$day[aout.all$missMatrix[,32] & !zero.count ]
         )


  zero.count <- aout.all.c$imputations[[1]]$heavyheavy_r3 == 0

  xyplot(
         aout.all.c$imputations[[1]]$hh_weight_r3[aout.all.c$missMatrix[,32] & !zero.count ]
         ~
         aout.all.c$imputations[[1]]$tod[aout.all.c$missMatrix[,32] & !zero.count ] |  aout.all.c$imputations[[1]]$day[aout.all.c$missMatrix[,32] & !zero.count ]
         )

  xyplot(ts ~ ts, groups = (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)),  data=df.wim.agg)

  xyplot(ts ~ ts, groups = (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)),  data=df.wim.agg.gapfill)
png(file = "wim.imputetests.2008.site.31.%03d.png",
      width=1200, height=1200, bg="transparent")

  xyplot(ts ~ jitter(unclass(ts)) | (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE, data=df.wim.year)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.year.gapfill)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.j.nz)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.j.nz.gapfill)

dev.off()



}
## the following are not used but interesting for historical purposes

process.wim <- function(df.wim){

  ## actually, first run some diagnostic stuff, check if length adds up
  ## to axle spacing, etc

  ## ts is not used at the moment
  ## wim.ts <- as.POSIXlt(strptime(df.wim$ts,"%Y-%m-%d %H:%M:%S"))


  df.wim$truck <- ifelse(df.wim$veh_class>3,TRUE,FALSE)
  df.wim$total.axle.spacing <-
    ifelse(!is.na(df.wim$axle_8_9_spacing),
           df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing+df.wim$axle_7_8_spacing+df.wim$axle_8_9_spacing,
           ifelse(!is.na(df.wim$axle_7_8_spacing),
                  df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing+df.wim$axle_7_8_spacing,
                  ifelse(!is.na(df.wim$axle_6_7_spacing),
                         df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing,
                         ifelse(!is.na(df.wim$axle_5_6_spacing),
                                df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing,
                                ifelse(!is.na(df.wim$axle_4_5_spacing),
                                       df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing,
                                       ifelse(!is.na(df.wim$axle_3_4_spacing),
                                              df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing,
                                              ifelse(!is.na(df.wim$axle_2_3_spacing),
                                                     df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing,
                                                     ifelse(!is.na(df.wim$axle_1_2_spacing),df.wim$axle_1_2_spacing
                                                            ))))))))

  df.wim$sum.axle.weight <-
    ifelse(!is.na(df.wim$axle_9_rt_weight),
           df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight+df.wim$axle_8_lt_weight+df.wim$axle_9_lt_weight,
           ifelse(!is.na(df.wim$axle_8_rt_weight),
                  df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight+df.wim$axle_8_lt_weight,
                  ifelse(!is.na(df.wim$axle_7_rt_weight),
                         df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight,
                         ifelse(!is.na(df.wim$axle_6_rt_weight),
                                df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight,
                                ifelse(!is.na(df.wim$axle_5_rt_weight),
                                       df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight ,
                                       ifelse(!is.na(df.wim$axle_4_rt_weight),
                                              df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight,
                                              ifelse(!is.na(df.wim$axle_3_rt_weight),
                                                     df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight ,
                                                     ifelse(!is.na(df.wim$axle_2_rt_weight),
                                                            df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight ,
                                                            ifelse(!is.na(df.wim$axle_1_rt_weight),
                                                                   df.wim$axle_1_lt_weight)))))))))





  ## how many axles?
  df.wim$total.axles <-
    ifelse(!is.na(df.wim$axle_9_lt_weight),9,
           ifelse(!is.na(df.wim$axle_8_lt_weight),8,
                  ifelse(!is.na(df.wim$axle_7_lt_weight),7,
                         ifelse(!is.na(df.wim$axle_6_lt_weight),6,
                                ifelse(!is.na(df.wim$axle_5_lt_weight),5,
                                       ifelse(!is.na(df.wim$axle_4_lt_weight),4,
                                              ifelse(!is.na(df.wim$axle_3_lt_weight),3,
                                                     ifelse(!is.na(df.wim$axle_2_lt_weight),2,
                                                            ifelse(!is.na(df.wim$axle_1_lt_weight),1,NA)))))))))
  ## code up the ARB classification scheme
  df.wim$heavyheavy <- ifelse(df.wim$gross_weight>=33 | ! is.na(df.wim$axle_3_4_spacing), TRUE, FALSE)

  ## just look at "illegal" axle weights
  df.wim$simple.overweight.index <- df.wim$gross_weight/df.wim$total.axles>18
  df.wim$overweight.index <- (
                       df.wim$axle_1_lt_weight + df.wim$axle_1_rt_weight > 18 |
                       df.wim$axle_2_lt_weight + df.wim$axle_2_rt_weight > 18 |
                       (df.wim$total.axles>2 & df.wim$axle_3_lt_weight + df.wim$axle_3_rt_weight > 18) |
                       (df.wim$total.axles>3 & df.wim$axle_4_lt_weight + df.wim$axle_4_rt_weight > 18) |
                       (df.wim$total.axles>4 & df.wim$axle_5_lt_weight + df.wim$axle_5_rt_weight > 18) |
                       (df.wim$total.axles>5 & df.wim$axle_6_lt_weight + df.wim$axle_6_rt_weight > 18) |
                       (df.wim$total.axles>6 & df.wim$axle_7_lt_weight + df.wim$axle_7_rt_weight > 18) |
                       (df.wim$total.axles>7 & df.wim$axle_8_lt_weight + df.wim$axle_8_rt_weight > 18) |
                       (df.wim$total.axles>8 & df.wim$axle_9_lt_weight + df.wim$axle_9_rt_weight > 18)
                       )


  df.wim
}
get.axle.weights <- function(df.wim){
  ## check axle loadings
  axle.weight <- data.frame(axle=1,weight=df.wim$axle_1_lt_weight + df.wim$axle_1_rt_weight,axle_spacing=NA,total.axles=df.wim$total.axles)
  axle.weight <- rbind(axle.weight,data.frame(axle=2,weight=df.wim$axle_2_lt_weight + df.wim$axle_2_rt_weight, axle_spacing=df.wim$axle_1_2_spacing,total.axles=df.wim$total.axles))
  axle.weight <- rbind(axle.weight,data.frame(axle=3,weight=df.wim$axle_3_lt_weight[df.wim$total.axles>2] + df.wim$axle_3_rt_weight[df.wim$total.axles>2], axle_spacing=df.wim$axle_2_3_spacing[df.wim$total.axles>2],total.axles=df.wim$total.axles[df.wim$total.axles>2]))
  axle.weight <- rbind(axle.weight,data.frame(axle=4,weight=df.wim$axle_4_lt_weight[df.wim$total.axles>3] + df.wim$axle_4_rt_weight[df.wim$total.axles>3], axle_spacing=df.wim$axle_3_4_spacing[df.wim$total.axles>3],total.axles=df.wim$total.axles[df.wim$total.axles>3]))
  axle.weight <- rbind(axle.weight,data.frame(axle=5,weight=df.wim$axle_5_lt_weight[df.wim$total.axles>4] + df.wim$axle_5_rt_weight[df.wim$total.axles>4], axle_spacing=df.wim$axle_4_5_spacing[df.wim$total.axles>4],total.axles=df.wim$total.axles[df.wim$total.axles>4]))
  axle.weight <- rbind(axle.weight,data.frame(axle=6,weight=df.wim$axle_6_lt_weight[df.wim$total.axles>5] + df.wim$axle_6_rt_weight[df.wim$total.axles>5], axle_spacing=df.wim$axle_5_6_spacing[df.wim$total.axles>5],total.axles=df.wim$total.axles[df.wim$total.axles>5]))
  axle.weight <- rbind(axle.weight,data.frame(axle=7,weight=df.wim$axle_7_lt_weight[df.wim$total.axles>6] + df.wim$axle_7_rt_weight[df.wim$total.axles>6], axle_spacing=df.wim$axle_6_7_spacing[df.wim$total.axles>6],total.axles=df.wim$total.axles[df.wim$total.axles>6]))
  axle.weight <- rbind(axle.weight,data.frame(axle=8,weight=df.wim$axle_8_lt_weight[df.wim$total.axles>7] + df.wim$axle_8_rt_weight[df.wim$total.axles>7], axle_spacing=df.wim$axle_7_8_spacing[df.wim$total.axles>7],total.axles=df.wim$total.axles[df.wim$total.axles>7]))
  axle.weight <- rbind(axle.weight,data.frame(axle=9,weight=df.wim$axle_9_lt_weight[df.wim$total.axles>8] + df.wim$axle_9_rt_weight[df.wim$total.axles>8], axle_spacing=df.wim$axle_8_9_spacing[df.wim$total.axles>8],total.axles=df.wim$total.axles[df.wim$total.axles>8]))
  axle.weight <- axle.weight[!is.na(axle.weight$weight),]
  axle.weight
}


load.wim.pair.data <- function(wim.ids,vds.nvars,lanes=0){
    bigdata <- data.frame()
  if(length(wim.ids)<1){
    print('no wim neighbors in database')
    couch.set.state(year,vds.id,list('truck_imputation_failed'='0 records in wim neighbor table'),local=localcouch)
    stop()
  }
  ## keep either the max number of lanes group, or all sites that have
  ## more or equal to number of lanes
  more.lanes <- wim.ids$lanes >= lanes
  if(length(wim.ids[more.lanes,1])<1){
    ## then just use the max lanes group
    maxlanes = wim.ids$lanes[1]
    more.lanes <- wim.ids$lanes >= maxlanes
  }
  wim.ids <- wim.ids[more.lanes,]
  ready.wimids = list()
  spd.pattern <- "(^sl1$|^sr\\d$)"

    checked.already=c(1)
  for( wii in 1:length(wim.ids$wim_id) ){
    wim.id <- wim.ids[wii,'wim_id']
    wim.dir <- wim.ids[wii,'direction']
    wim.lanes <- wim.ids[wii,'lanes']
    ## make sure that there are the *correct* nubmer of variables
    ## there should be 14 for each lane, and then 4 for the left
    ## lane, if there are more than two lanes, then 3 for time variables

    paired.vdsids <- wim.vds.pairs[wim.vds.pairs$wim_id==wim.id  & wim.vds.pairs$direction==wim.dir,'vds_id']
    print(paired.vdsids)
    for(paired.vdsid in paired.vdsids){
      if(paired.vdsid %in% checked.already) { next }
      checked.already <- c(checked.already,paired.vdsid)
      paired.RData <- get.RData.view(paired.vdsid,year)
      if(length(paired.RData)==0) { next }
      result <- couch.get.attachment(trackingdb,paired.vdsid,paired.RData,local=localcouch)
      ## load(result)
      ## loading now happens in couch.get.attachment
      df.merged <- tempfix.borkborkbork(df.merged)
      if(dim(df.merged)[1] < 100){
        print(paste('pairing for',paired.vdsid,year,'pretty empty'))
        next
      }
      ic.names <- names(df.merged)
      shouldbe <-
        (wim.lanes * 2) + ## n and o for each lane
          (2 * 10) +  ## two right hand lanes should have 10 truck vars
            (wim.lanes * 2) + ## each lane has speed and count from summary report
              3 ## ts, tod, day
      ## add for speed too if in the data set
      speed.vars <- grep( pattern=spd.pattern,x=ic.names ,perl=TRUE,value=TRUE,invert=FALSE)
      if(length(speed.vars)>0){
        shouldbe <- shouldbe + (wim.lanes) # one speed measurement for each lane
      }

      if(dim(df.merged)[2]<shouldbe){
        print(paste('pairing for',paired.vdsid,year,'missing some variables, expected',shouldbe,'got',dim(df.merged)[2]))
        print(names(df.merged))
        next
      }
      print(paste('processing',paired.vdsid,year))
      ready.wimids[length(ready.wimids)+1]=wim.ids[wii,]
      ## convention over configuration I guess.  These files are always called df.merged
      keep.columns = intersect(c( "ts","tod","day","imp","vds_id" ),ic.names)
      ## use vds.nvars to drop unwanted lanes
      for( lane in 1:length(vds.nvars) ){
        pattern = paste(substring(vds.nvars[lane],2)[1],'$',sep='')
        keep.columns.lane <-  grep( pattern=pattern,x=ic.names,perl=TRUE,value=TRUE)
        if(length(keep.columns) == 0){
          keep.columns = keep.columns.lane
        }else{
          keep.columns = union(keep.columns,keep.columns.lane)
        }
      }
      df.merged <- df.merged[,keep.columns]
      ## merge it
      if(length(bigdata)==0){
        bigdata <-  df.merged
      }else{
        ## here I need to make sure all WIM-VDS sites have similar lanes
        ## the concern is a site with *fewer* lanes than the vds site
        ic.names <- names(df.merged)
        bigdata.names <- names(bigdata)
        ## keep the larger of the two
        common.names <- intersect(ic.names,bigdata.names)
        bigdata <- bigdata[,common.names]
        df.merged <- df.merged[,common.names]
        bigdata <- rbind( bigdata, df.merged )
      }
      rm(df.merged)

    }
  }
    if(length(ready.wimids)>0){
      ready.wimids <- unique(ready.wimids)
      couch.set.state(year,vds.id,list('wim_neighbors_ready'=ready.wimids),local=localcouch)
    }

  bigdata
}
