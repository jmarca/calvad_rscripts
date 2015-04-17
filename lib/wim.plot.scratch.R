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
png(filename = "wim.imputetests.2008.site.31.%03d.png",
      width=1200, height=1200, bg="transparent")

  xyplot(ts ~ jitter(unclass(ts)) | (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE, data=df.wim.year)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.year.gapfill)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.j.nz)
  xyplot(ts ~ jitter(unclass(ts))| (is.na(truck.r1) | is.na(truck.r2) | is.na(truck.r3)), jitter.data=TRUE,  data=df.wim.j.nz.gapfill)

dev.off()
}
