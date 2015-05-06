### you need to have an existing value for con, a dbConnect object,
### and pass that in to the various functions that require it

##' Load WIM data straight from the DB, bypassing any existing RData files
##'
##' Although RData file will be generated, it will not be used.
##'
##' This is actually a wrappter around \code{get.wim.site.2}
##'
##'
##' @title load.wim.data.straight
##' @param wim.site the WIM site number
##' @param year the year
##' @param con the connection to postgresql
##' @return the output from get.wim.site.2, which should be a data
##' frame containing the WIM data.
##' @author James E. Marca
load.wim.data.straight <- function(wim.site,year,con){
  start.wim.time <-  paste(year,"-01-01",sep='')
  end.wim.time <-   paste(year+1,"-01-01",sep='')
  get.wim.site.2(wim.site,
                 start.time=start.wim.time,
                 end.time=end.wim.time,
                 con)
}


##' Get WIM speed data from the database (SQL)
##'
##' The WIM data also has summary reports.  These reports contain speed information on all vehicles in every lane, whereas the usual WIM data discards non-truck data.  This command hits the database to query those summary repots for this WIM site.
##' @title get.wim.speed.from.sql
##' @param wim.site the WIM site number
##' @param year the year
##' @param con the connection to postgresql
##' @return a speed dataframe, from a call to get.wim.site.speed
##' @author James E. Marca
get.wim.speed.from.sql <- function(wim.site,year,con){
  ## load wim data
  ## by year
  start.wim.time <-  paste(year,"-01-01",sep='')
  end.wim.time <-   paste(year+1,"-01-01",sep='')
  df.speed <-  get.wim.site.speed(wim.site,
                                  start.time=start.wim.time,
                                  end.time=end.wim.time,
                                  con)
  df.speed <-  wim.recode.lanes(df.speed)
  df.speed
}

##' Get the directions of movement for a WIM site
##'
##' Each WIM site has detectors that might cover more than one
##' direction.  Unlike the VDS ids, a single WIM site number can refer
##' to flow on both sides of a highway or freeway.  This function
##' queries the database to get all of the directions.  I've seen one,
##' two and three directions.
##' @title get.wim.directions
##' @param wim.site the WIM site number
##' @param con a connection to postgresql
##' @return a simple dataframe containing the result of the DB query,
##' with a column for the direction.  Only applies to the passed in
##' WIM site, of course.
##' @author James E. Marca
get.wim.directions <- function(wim.site,con){
  wim.query <- paste("select distinct direction as direction from wim_lane_dir where site_no=",wim.site,sep='')
  print(wim.query)
  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df.wim <- RPostgreSQL::fetch(rs,n=-1)
  df.wim
}


##' Get a listing of all WIM sites
##'
##' I used to use this more when I set up all the operations in R.
##' Now that I use node.js to call R, I don't really use this.  Still,
##' may be useful.  What it does is call the DB and get a list of all
##' of the WIM sites with more than one lane and that aren't the
##' PrePass type.
##' @title get.list.wim.sites
##' @param con a connection to postgresql
##' @return the dataframe containing the results of the query, with columns for
##'   site_no,
##'   loc (location name)
##'   lanes (the total number of lanes (one or more directions of flow))
##'   vendor (in CAlifornia there used to be PAT and IRD, now just IRD)
##'   wim_type The WIM station type, excluding all PrePass stations
##' @author James E. Marca
get.list.wim.sites <-  function(con){
  #   wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations join wim_district on (wim_id=site_no) where district_id in (7,8,11,12) and lanes>1 and wim_type != 'PrePass'" )
  #   wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations join wim_district on (wim_id=site_no) where district_id in (1,2,3,4,5,6,9,10) and lanes>1 and wim_type != 'PrePass'" )
  wim.query <- paste( "select distinct site_no,loc,lanes,vendor,wim_type from wim_stations where lanes>1 and wim_type != 'PrePass'" )
  print(wim.query)
  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df.wim <- RPostgreSQL::fetch(rs,n=-1)
  df.wim
}

##' Get the WIM sites
##'
##' Just the sites, no other detail
##'
##'
##' @title get.wim.sites
##' @param con the connection to postgresql
##' @return a dataframe with one column called wim_id, which is
##' actually the "site_no" Not sure why I changed the name, but I did
##' this a long time ago
##' @author James E. Marca
get.wim.sites <- function(con){
  wim.query <- "select distinct site_no as wim_id from wim_lane_dir order by site_no"
  print(wim.query)
  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df.wim <- RPostgreSQL::fetch(rs,n=-1)
  df.wim
}

##' Get a list of neighboring VDS sites
##'
##' Used to have more complicated logic here, with the idea of get vds
##' sites, then go back with a second query to get any and all WIM
##' sites that are neigbors
##'
##' Now the current SQL query is just a straight select distinct sites
##' from the vds_wim_neighbors table
##'
##' @title get.list.neighbor.vds.sites
##' @param con the connection to postgresql
##' @return a dataframe containing a single column of VDS id called
##' vds_id.  These VDS ids are all of the ids that are in the
##' vds_wim_neighbors list.
##' @author James E. Marca
get.list.neighbor.vds.sites <- function(con){

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
  rs <- RPostgreSQL::dbSendQuery(con,db.query)
  df.q <- RPostgreSQL::fetch(rs,n=-1)
  df.q
}


##' Get the list of WIM sites that are in the vds_wim_neighbors, but
##' more complicated than that
##'
##' And now the second half of the above query.  Pass the VDS id and
##' to get any and all neighboring WIM sites to use
##'
##' @title get.list.neighbor.wim.istes
##' @param vds.id the VDS site id.  Should be already in the neighbor
##' table.  If not, then you need to stick it there.
##' @param con the connection to postgresql
##' @return a dataframe containing columns for
##'    wim_id the WIM site number
##'    dist the distance from the WIM site to the VDS detector (straight line, not network distance)
##'    direction the direction of flow of the neighbor WIM site
##'    lanes the total number of lanes at the WIM site
##' @author James E. Marca
get.list.neighbor.wim.sites <- function(vds.id,con){

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
  rs <- RPostgreSQL::dbSendQuery(con,db.query)
  df.q <- RPostgreSQL::fetch(rs,n=-1)
  df.q
}


##' get the list of neighbor WIM sites, limited by the same district
##'
##' This one is more restrictive query, getting WIM sites that share
##' the same district and that have the same direction of flow as the
##' VDS site.  This doesn't work all the time because you run out of
##' sites pretty fast.  I actually do not recall at this time whether
##' this one is used or the above more genearl one
##' @title get.lsit.district.neighbor.wim.sites
##' @param vdsid the VDS id
##' @param con the connection to postgresql
##' @return a dataframe containing columns for
##'    wim_id the WIM site number
##'    dist the distance from the WIM site to the VDS detector (straight line, not network distance)
##'    direction the direction of flow of the neighbor WIM site
##'    lanes the total number of lanes at the WIM site
##' @author James E. Marca
get.list.district.neighbor.wim.sites <- function(vdsid,con){
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
  rs <- RPostgreSQL::dbSendQuery(con,query)
  df.wim <- RPostgreSQL::fetch(rs,n=-1)
  df.wim
}


##' get a list of vds-wim pairs
##'
##' The closest part is a bit redundant.  The logic of the truck
##' imputation is that close by VDS and WIM sites are "paired", and we
##' pretend that it is a single detector site with both truck and VDS
##' measurements.
##'
##' This function gets all of these pairs all at once
##'
##' @title get.list.cloest.wim.pairs
##' @param con the connection to postgresql
##' @return a dataframe containing columns for vds_id, wim_id, and direction
##' @author James E. Marca
get.list.closest.wim.pairs <- function(con){

  wim.query <- paste("select vds_id,wim_id,direction from imputed.vds_wim_pairs")

  print(wim.query)
  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df.q <- RPostgreSQL::fetch(rs,n=-1)
  df.q
}


##' Get list of vds-wim pairs, but regenerate it
##'
##' this code runs the original pairing query, which actually won't
##' reproduce the vds-wim pairs table anymore.  Here for historical
##' interest more than anything else.
##'
##' @title get.list.regenerate.wim.pairs
##' @param wimid the WIM station
##' @param direction the directon of flow
##' @param samefreeway Whether to force matches to just the same freeway.  TRUE or FALSE
##' @param con the connection to postgresql
##' @return a dataframe containing vds_id, wim_id, distance, freeway, direction
##' @author James E. Marca
get.list.regenerate.wim.pairs <- function(wimid,direction,samefreeway=TRUE,con){

    joinfreeway <- ''
    if(samefreeway){
        joinfreeway <- "join wim_freeway wf on (wf.wim_id=b.site_no and wf.freeway_id=b.freeway)"

    }
  wim.query <- paste("select b.vds_id as vds_id,"
                     ,"b.site_no as wim_id ,"
                     ,"b.dist as distance,"
                     ,"b.freeway as freeway,"
                     ,"b.direction as direction"
                     ,"from  imputed.vds_wim_neighbors b"
                     ,joinfreeway
                     ,"join vds_vdstype a on (b.vds_id=a.vds_id and type_id='ML')"
                     ,"where b.site_no=",wimid
                     ,paste("and b.direction=newctmlmap.canonical_direction('",direction,"')",sep='')
                     ,"order by dist limit 50")

  print(wim.query)
  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df.q <- RPostgreSQL::fetch(rs,n=-1)
  df.q

}


##' Get the WIM site speed.
##'
##' a query that gets speed from the summaries_5min_speed table.
##'
##' @title get.wim.site.speed
##' @param wim.siteno the WIM site
##' @param start.time the starting time for the query
##' @param end.time the ending time for the query.
##' @param con a connection to postgresql
##' @return a dataframe containing ts,lane,speed,count,direction So
##' you can weight speed by the count in each lane
##' @author James E. Marca
get.wim.site.speed <- function(wim.siteno,
                               start.time,
                               end.time,
                               con){
  query <- paste(
                 "select extract(epoch from ts) as ts, b.lane_no as lane, veh_speed, veh_count, direction from  wim.summaries_5min_speed a join wim_lane_dir b on (b.site_no=a.site_no and  b.wim_lane_no=a.wim_lane_no) where  ts>='",start.time,"' and ts < '",end.time, "' and a.site_no=",wim.siteno)

  print(query)

  rs <- RPostgreSQL::dbSendQuery(con,query)
  df <- RPostgreSQL::fetch(rs,n=-1)
  df$ts <- as.POSIXct(unclass(df$ts) + ISOdatetime(1970,1,1,0,0,0, tz="UTC"), tz="UTC" )
  RPostgreSQL::dbClearResult(rs)
  rm('rs')
  df
}


##' Get wim site data
##'
##' This is how the WIM data is retrieved from the database.  All the
##' magic happens here to convert the measurements into the rows in
##' the dataframes
##'
##' @title get.wim.site.2
##' @param wim.siteno the WIM site
##' @param start.time starting time
##' @param end.time ending time
##' @param con a connection to postgresql
##' @return a dataframe containing WIM data
##' @author James E. Marca
get.wim.site.2 <- function(wim.siteno,
                           start.time='2007-01-01',
                           end.time='2008-01-01',
                           con){
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

  rs <- RPostgreSQL::dbSendQuery(con,wim.query)
  df <- RPostgreSQL::fetch(rs,n=-1)
  df$ts <- as.POSIXct(unclass(df$ts) + ISOdatetime(1970,1,1,0,0,0, tz="UTC") ,tz='UTC')
  RPostgreSQL::dbClearResult(rs)
  rm(rs)
  df
}

##' get wim site extremes.  This does nothing right now
##'
##' @title get.wim.site.extremes
##' @param wim.siteno the site
##' @param start.time the start to look
##' @param end.time the end to look
##' @param con a connection to postgresql
##' @return nothing at alldocs
##' @author James E. Marca
get.wim.site.extremes <- function(wim.siteno,
                                  start.time,
                                  end.time,
                                  con){
}

##' Get WIM status, which tells you if the site is good data or unusable
##'
##'
##' @title get.wim.status
##' @param wim.site the WIM site number
##' @param con a connection to postgresql
##' @return a dataframe containing site_no, ts, class_status, class_notes,weight_status,weight_notes.  Stupidly, I used to limit the query to below 2009! but no more
##' @author James E. Marca
get.wim.status <- function(wim.site,con){
  ## it is important to ignore bad data
  wim.status.query <-
    paste("select site_no , ts , class_status , class_notes , weight_status , weight_notes from wim_status where site_no=",wim.site)

  rs <- RPostgreSQL::dbSendQuery(con,wim.status.query)
  df.wim.status <- RPostgreSQL::fetch(rs,n=-1)
  df.wim.status
}


##'  create additional variables for wim sites
##'
##' wim.additional.variables
##' @param df.wim the WIM dataframe
##' @return the augmented dataframe
##' @author James E. Marca
wim.additional.variables <- function(df.wim){

  ## I want to parameterize the additional variables here, but can't see how


  ## df.wim$gross_weight        df.wim$overweight          df.wim$total_axles         df.wim$truck               df.wim$veh_len
  ## df.wim$heavyheavy          df.wim$site_no             df.wim$total_axle_spacing  df.wim$ts                  df.wim$veh_no
  ## df.wim$lane                df.wim$speed               df.wim$total_axle_weight   df.wim$veh_class           df.wim$violation_code
  ##  I used ot have df.wim$truck, but now in the sql that is all I have
  ## no more of this kind of line
  ## df.wim$over_weight <- ifelse(df.wim$truck && df.wim$overweight, df.wim$gross_weight,NA)

  df.names <- names(df.wim)
  if('overweight' %in% df.names){
    df.wim$over_weight <- ifelse(df.wim$overweight, df.wim$gross_weight,NA)
    ## df.wim$legal_weight <- ifelse(!df.wim$overweight, df.wim$gross_weight,NA)
    ## that is wrong, I think
  }
  if('heavyheavy' %in% df.names){
    df.wim$hh_weight <- ifelse(df.wim$heavyheavy, df.wim$gross_weight,NA)
    df.wim$hh_axles <- ifelse(df.wim$heavyheavy, df.wim$total_axles,NA)
    df.wim$hh_len <- ifelse(df.wim$heavyheavy, df.wim$total_axle_spacing,NA)
    df.wim$hh_speed <- ifelse(df.wim$heavyheavy, df.wim$speed,NA)

    df.wim$nh_weight <- ifelse(!df.wim$heavyheavy, df.wim$gross_weight,NA)
    df.wim$nh_axles <- ifelse(!df.wim$heavyheavy, df.wim$total_axles,NA)
    df.wim$nh_len <- ifelse(!df.wim$heavyheavy, df.wim$total_axle_spacing,NA)
    df.wim$nh_speed <- ifelse(!df.wim$heavyheavy, df.wim$speed,NA)
  }

  if('empty' %in% df.names){
    df.wim$mt_weight <- ifelse(df.wim$empty, df.wim$gross_weight,NA)
    ## df.wim$mt_axles <- ifelse(df.wim$empty, df.wim$total_axles,NA)
    ## df.wim$mt_len <- ifelse(df.wim$empty, df.wim$total_axle_spacing,NA)
    df.wim$mt_speed <- ifelse(df.wim$empty, df.wim$speed,NA)
  }

  ## recode some names here to make naming scheme consistent.
  orig.names <- names(df.wim)
  orig.names[match(c("gross_weight","total_axles","total_axle_spacing","speed"),orig.names)]<- c("tr_weight","tr_axles","tr_len","tr_speed")
  names(df.wim) <- orig.names

  ## finally, I don't need total_axle_weight at this time
  ta.weight <-  grep( pattern='total_axle_weight',x=names(df.wim) ,perl=TRUE,value=TRUE)
  for(col in ta.weight){
    df.wim[,col] <- NULL
  }
  df.wim
}



##' Process the WIM site (version 2)
##'
##' need to parameterize count variables here such that ,count.vars=c("truck","heavyheavy","overweight")
##'
##' does things like wipe out weight, etc if the status is bad
##'
##' also winds up heavyheavy, and a trial variable for empty trucks
##'
##' @title process.wim.2
##' @param df.wim the WIM data
##' @return the augmented WIM dataframe
##' @author James E. Marca
process.wim.2 <- function(df.wim){

  df.wim$total_axle_weight[df.wim$weight_status=='B'] <- NA
  df.wim$gross_weight[df.wim$weight_status=='B'] <- NA
  df.wim$veh_class[df.wim$class_status=='B'] <- NA

  ## negative weights are stupid
  neg.weight <- df.wim$gross_weight < 0
  df.wim$gross_weight[neg.weight] <- NA  ## make missing

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

##' get an appropriate list of lane names, with consistent naming with
##' the VDS sites
##'
##' @title wim.lane.numbers
##' @param lanes the number of lanes
##' @return a list of lane names to rename the current lanes
##' @author James E. Marca
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

##' recode the WIM lanes to have the usual names
##'
##' @title wim.recode.lanes
##' @param df the WIM dataframe
##' @return the modified data frame with the lane names changed
##' @author James E. Marca
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



##' Make a pretty histogram of WIM data
##'
##' @title pretty.thistogram
##' @param x the data
##' @param main the main title
##' @param xlab xaxis label
##' @param ylab yaxis label
##' @return the output of the plotting calls
##' @author James E. Marca
pretty.thistogram <- function(x,main="Histogram",xlab="",ylab=""){
  ## get rid of NA values
  x <- x[!is.na(x),]
  MASS::truehist( x, xlab=xlab,main=main )
                                        # calculate density estimation
  d <- density( x )

  abline( h = axTicks(2) , col = "gray", lwd = .5 )
                                        # adds a tick and translucent line to display the density estimate
  lines( d, lwd = 4, col = "#66666688" )
                                        # add a rug (also using translucency)
  rug( x, col = "#00000088" )
}


##' A plot of smooth color gradations scatter plot
##'
##' calls smoothScatter with some parameters
##' @title smooth.color.scatter
##' @param x the data to plot
##' @param main the main title of the plot
##' @param xlab the xaxis label
##' @param ylab the y axis label
##' @return nothing, generates plots as a side effect
##' @author James E. Marca
smooth.color.scatter <- function(x,main="Histogram",xlab="",ylab=""){
  smoothScatter(x, nrpoints=floor(0.0001*length(x[,1])),
                colramp=colorRampPalette( c("white","red")),
                transformation = function(x) {x^0.25},
                main=main,xlab=xlab,ylab=ylab  )

  ## a different color scheme:
  ##  Lab.palette <- colorRampPalette(c("white","blue", "orange", "red"), space = "Lab")
  ##  smoothScatter(x, colramp = Lab.palette)
}

##' Generate WIM plots
##'
##' not yet modified to use ggplot2
##'
##' @title wim.plot.gen
##' @param file the output file name
##' @param wim.site the WIM site
##' @param df.wim the data
##' @param width default to 850
##' @param height default to 850
##' @param ... other parameters to pass to the plot functions
##' @return nothing.  run for the side effect of generating plots
##' @author James E. Marca
wim.plot.gen <- function(file,wim.site,df.wim,width=850,height=850,...){

  png(filename = file,
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

  lattice::xyplot(df.wim$gross_weight ~ df.wim$total_axle_weight | factor(paste(df.wim$total_axles,"axle trucks")),
         panel=function(x,y){
           lattice::panel.smoothScatter(x,y,nbin=c(200,200))
         })

  lattice::xyplot(df.wim$gross_weight ~ df.wim$total_axle_spacing | factor(ifelse(df.wim$heavyheavy,"heavy heavy-duty","other")),
         panel=function(x,y){
           lattice::panel.smoothScatter(x,y,nbin=c(200,200))
         })
  pretty.thistogram(df.wim$gross_weight/df.wim$total_axles,main=paste("Histogram of WIM gross_weight/axles for all vehicles, at site",wim.site),xlab="gross_weight / total axles")
  pretty.thistogram(df.wim$gross_weight[df.wim$overweight]/df.wim$total_axles[df.wim$overweight],main=paste("Histogram of WIM gross_weight/axles for vehicles w/ at least one axle > 18kips , at site",wim.site),xlab="gross_weight / total axles")
  pretty.thistogram(df.wim$gross_weight[!df.wim$overweight]/df.wim$total_axles[!df.wim$overweight],main=paste("Histogram of WIM gross_weight/axles for vehicles with <= 18kips per axle, at site",wim.site),xlab="gross_weight / total axles")

  ##   bwplot(axle ~ weight | factor(paste(total_axles,"axle trucks")),data=axle.weight,xlab="axle loading, kips")


  pretty.thistogram(df.wim$veh_len,main=paste("Histogram of WIM vehicle length, at site",wim.site),xlab="measured vehicle length, feet")

  pretty.thistogram(df.wim$total_axle_spacing,main=paste("Histogram of WIM total inter-axle spacing, at site",wim.site),xlab="cumulative inter-axle spacing per vehicle, feet")

  ## lattice::xyplot(df.wim$veh_len ~ df.wim$total_axle_spacing, panel = function(x,y){
  ##   lattic::panel.smoothScatter(x,y,nbin=c(400,400))
  ## },main=paste("Length vs  inter-axle spacing, at site",wim.site),ylab="measured vehicle length, feet", xlab="total inter-axle spacing per vehicle, feet")

  dev.off()
}


##' The Amelia output post impute plots
##'
##' Amelia offers some plots out of the box.  This routine triggers
##' the more interesting of those.
##'
##' The tscs plot function is probably the more interesting one.
##' Plots the data over time and cs with a measure of teh variablility
##' of the imputation
##'
##' @title aout.postimp.plots
##' @param file the output file name
##' @param wim.site the WIM site number
##' @param aout the amelia output
##' @param width the width
##' @param height the height
##' @param ... more stuff to pass to the plot commands
##' @return nothing.  Side effect is generating the plots
##' @author James E. Marca
aout.postimp.plots <- function(file,wim.site,aout,width=850,height=850,...){

  ts.lt <- as.POSIXlt(aout$imputations[[1]]$ts[1])
  year  <- ts.lt$year

  if(is.null(file)){

    file <- paste ('wim.imputed.',1900+year,'.',wim.site,'.%03d.png',sep='');

  }
  png(filename = "wim.imputetests.2008.site.31.%03d.png",
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

  ##      Amelia::tscsPlot(output, var, cs, draws = 100, conf = .90,
  ##            misscol = "red", obscol = "black", xlab, ylab, main,
  ##            pch, ylim, xlim, ...)
  week.days <- c('Sun','Mon','Tue','Wed','Thu','Fri','Sat')
  ## plots that might be useful
  day <- 0
  for(day in 0:6){
    ## Amelia::tscsPlot(aout,'truck_r1',day,xlab='time of day',ylab='truck count, lane Right 1', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )
    ## Amelia::tscsPlot(aout,'truck_r2',day,xlab='time of day',ylab='truck count, lane Right 2', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )
    ## Amelia::tscsPlot(aout,'truck_r3',day,xlab='time of day',ylab='truck count, lane Right 3', main=paste('Imputed truck counts for wim site',wim.site,'on',week.days[day+1],'in',year) )

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
    Amelia::tscsPlot(aout,'truck_r1',day,xlab='time of day',ylab='trucks, lane Right 1', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'truck_r2',day,xlab='time of day',ylab='trucks, lane Right 2', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'truck_r3',day,xlab='time of day',ylab='trucks, lane Right 3', main=paste('trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'tr_weight_r1',day,xlab='time of day',ylab='sum weight of trucks, lane Right 1', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'tr_weight_r2',day,xlab='time of day',ylab='sum weight of trucks, lane Right 2', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'tr_weight_r3',day,xlab='time of day',ylab='sum weight of trucks, lane Right 3', main=paste('Imputed weight of trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'heavyheavy_r1',day,xlab='time of day',ylab='hh trucks, lane Right 1', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'heavyheavy_r2',day,xlab='time of day',ylab='hh trucks, lane Right 2', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )
    Amelia::tscsPlot(aout,'heavyheavy_r3',day,xlab='time of day',ylab='hh trucks, lane Right 3', main=paste('heavy heavy trucks wim site',wim.site,'on',week.days[day+1],'in',year) )

  }
  dev.off()
}


## the following are not used but interesting for historical purposes

## ##' Process WIM the old way
## ##'
## ##' not used anymore, here for historical learnings
## ##' @title process.wim
## ##' @param df.wim the WIM data
## ##' @return the modified WIM dataframe
## ##' @author James E. Marca
## process.wim <- function(df.wim){

##   ## actually, first run some diagnostic stuff, check if length adds up
##   ## to axle spacing, etc

##   ## ts is not used at the moment
##   ## wim.ts <- as.POSIXlt(strptime(df.wim$ts,"%Y-%m-%d %H:%M:%S"))


##   df.wim$truck <- ifelse(df.wim$veh_class>3,TRUE,FALSE)
##   df.wim$total.axle.spacing <-
##     ifelse(!is.na(df.wim$axle_8_9_spacing),
##            df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing+df.wim$axle_7_8_spacing+df.wim$axle_8_9_spacing,
##            ifelse(!is.na(df.wim$axle_7_8_spacing),
##                   df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing+df.wim$axle_7_8_spacing,
##                   ifelse(!is.na(df.wim$axle_6_7_spacing),
##                          df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing+df.wim$axle_6_7_spacing,
##                          ifelse(!is.na(df.wim$axle_5_6_spacing),
##                                 df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing+df.wim$axle_5_6_spacing,
##                                 ifelse(!is.na(df.wim$axle_4_5_spacing),
##                                        df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing+df.wim$axle_4_5_spacing,
##                                        ifelse(!is.na(df.wim$axle_3_4_spacing),
##                                               df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing+df.wim$axle_3_4_spacing,
##                                               ifelse(!is.na(df.wim$axle_2_3_spacing),
##                                                      df.wim$axle_1_2_spacing+df.wim$axle_2_3_spacing,
##                                                      ifelse(!is.na(df.wim$axle_1_2_spacing),df.wim$axle_1_2_spacing
##                                                             ))))))))

##   df.wim$sum.axle.weight <-
##     ifelse(!is.na(df.wim$axle_9_rt_weight),
##            df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight+df.wim$axle_8_lt_weight+df.wim$axle_9_lt_weight,
##            ifelse(!is.na(df.wim$axle_8_rt_weight),
##                   df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight+df.wim$axle_8_lt_weight,
##                   ifelse(!is.na(df.wim$axle_7_rt_weight),
##                          df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight+df.wim$axle_7_lt_weight,
##                          ifelse(!is.na(df.wim$axle_6_rt_weight),
##                                 df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight+df.wim$axle_6_lt_weight,
##                                 ifelse(!is.na(df.wim$axle_5_rt_weight),
##                                        df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight+df.wim$axle_5_lt_weight ,
##                                        ifelse(!is.na(df.wim$axle_4_rt_weight),
##                                               df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight+df.wim$axle_4_lt_weight,
##                                               ifelse(!is.na(df.wim$axle_3_rt_weight),
##                                                      df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight+df.wim$axle_3_lt_weight ,
##                                                      ifelse(!is.na(df.wim$axle_2_rt_weight),
##                                                             df.wim$axle_1_lt_weight+df.wim$axle_2_lt_weight ,
##                                                             ifelse(!is.na(df.wim$axle_1_rt_weight),
##                                                                    df.wim$axle_1_lt_weight)))))))))





##   ## how many axles?
##   df.wim$total.axles <-
##     ifelse(!is.na(df.wim$axle_9_lt_weight),9,
##            ifelse(!is.na(df.wim$axle_8_lt_weight),8,
##                   ifelse(!is.na(df.wim$axle_7_lt_weight),7,
##                          ifelse(!is.na(df.wim$axle_6_lt_weight),6,
##                                 ifelse(!is.na(df.wim$axle_5_lt_weight),5,
##                                        ifelse(!is.na(df.wim$axle_4_lt_weight),4,
##                                               ifelse(!is.na(df.wim$axle_3_lt_weight),3,
##                                                      ifelse(!is.na(df.wim$axle_2_lt_weight),2,
##                                                             ifelse(!is.na(df.wim$axle_1_lt_weight),1,NA)))))))))
##   ## code up the ARB classification scheme
##   df.wim$heavyheavy <- ifelse(df.wim$gross_weight>=33 | ! is.na(df.wim$axle_3_4_spacing), TRUE, FALSE)

##   ## just look at "illegal" axle weights
##   df.wim$simple.overweight.index <- df.wim$gross_weight/df.wim$total.axles>18
##   df.wim$overweight.index <- (
##                        df.wim$axle_1_lt_weight + df.wim$axle_1_rt_weight > 18 |
##                        df.wim$axle_2_lt_weight + df.wim$axle_2_rt_weight > 18 |
##                        (df.wim$total.axles>2 & df.wim$axle_3_lt_weight + df.wim$axle_3_rt_weight > 18) |
##                        (df.wim$total.axles>3 & df.wim$axle_4_lt_weight + df.wim$axle_4_rt_weight > 18) |
##                        (df.wim$total.axles>4 & df.wim$axle_5_lt_weight + df.wim$axle_5_rt_weight > 18) |
##                        (df.wim$total.axles>5 & df.wim$axle_6_lt_weight + df.wim$axle_6_rt_weight > 18) |
##                        (df.wim$total.axles>6 & df.wim$axle_7_lt_weight + df.wim$axle_7_rt_weight > 18) |
##                        (df.wim$total.axles>7 & df.wim$axle_8_lt_weight + df.wim$axle_8_rt_weight > 18) |
##                        (df.wim$total.axles>8 & df.wim$axle_9_lt_weight + df.wim$axle_9_rt_weight > 18)
##                        )


##   df.wim
## }

## ##' Get the axle weights
## ##'
## ##' the WIM data has some stuff.  axle left, axle right, etc.  so
## ##' @title get.axle.weights
## ##' @param df.wim the WIM dataframe
## ##' @return a new dataframe with axle weights
## ##' @author James E. Marca
## get.axle.weights <- function(df.wim){
##   ## check axle loadings
##   axle.weight <- data.frame(axle=1,weight=df.wim$axle_1_lt_weight + df.wim$axle_1_rt_weight,axle_spacing=NA,total.axles=df.wim$total.axles)
##   axle.weight <- rbind(axle.weight,data.frame(axle=2,weight=df.wim$axle_2_lt_weight + df.wim$axle_2_rt_weight, axle_spacing=df.wim$axle_1_2_spacing,total.axles=df.wim$total.axles))
##   axle.weight <- rbind(axle.weight,data.frame(axle=3,weight=df.wim$axle_3_lt_weight[df.wim$total.axles>2] + df.wim$axle_3_rt_weight[df.wim$total.axles>2], axle_spacing=df.wim$axle_2_3_spacing[df.wim$total.axles>2],total.axles=df.wim$total.axles[df.wim$total.axles>2]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=4,weight=df.wim$axle_4_lt_weight[df.wim$total.axles>3] + df.wim$axle_4_rt_weight[df.wim$total.axles>3], axle_spacing=df.wim$axle_3_4_spacing[df.wim$total.axles>3],total.axles=df.wim$total.axles[df.wim$total.axles>3]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=5,weight=df.wim$axle_5_lt_weight[df.wim$total.axles>4] + df.wim$axle_5_rt_weight[df.wim$total.axles>4], axle_spacing=df.wim$axle_4_5_spacing[df.wim$total.axles>4],total.axles=df.wim$total.axles[df.wim$total.axles>4]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=6,weight=df.wim$axle_6_lt_weight[df.wim$total.axles>5] + df.wim$axle_6_rt_weight[df.wim$total.axles>5], axle_spacing=df.wim$axle_5_6_spacing[df.wim$total.axles>5],total.axles=df.wim$total.axles[df.wim$total.axles>5]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=7,weight=df.wim$axle_7_lt_weight[df.wim$total.axles>6] + df.wim$axle_7_rt_weight[df.wim$total.axles>6], axle_spacing=df.wim$axle_6_7_spacing[df.wim$total.axles>6],total.axles=df.wim$total.axles[df.wim$total.axles>6]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=8,weight=df.wim$axle_8_lt_weight[df.wim$total.axles>7] + df.wim$axle_8_rt_weight[df.wim$total.axles>7], axle_spacing=df.wim$axle_7_8_spacing[df.wim$total.axles>7],total.axles=df.wim$total.axles[df.wim$total.axles>7]))
##   axle.weight <- rbind(axle.weight,data.frame(axle=9,weight=df.wim$axle_9_lt_weight[df.wim$total.axles>8] + df.wim$axle_9_rt_weight[df.wim$total.axles>8], axle_spacing=df.wim$axle_8_9_spacing[df.wim$total.axles>8],total.axles=df.wim$total.axles[df.wim$total.axles>8]))
##   axle.weight <- axle.weight[!is.na(axle.weight$weight),]
##   axle.weight
## }
