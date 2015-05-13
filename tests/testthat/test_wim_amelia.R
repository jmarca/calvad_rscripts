config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','amelia','trials')
rcouchutils::couch.makedb(parts)

test_that("running amelia on wim is sane",{

    file  <- './files/37/S/wim.agg.RData'
    site_no <- 37
    direction <- 'S'
    year <- 2012
    seconds <- 120
    path <- './files/37/S'
    env <- new.env()
    res <- load(file=file,envir = env)
    df.wim <- env[[res]]

    count.pattern <- '^(not_heavyheavy|heavyheavy|count_all_veh_speed)'
    mean.pattern <- "_(weight|axle|len|speed)"
    mean.exclude.pattern <- "^(mean)"

    ic.names <- names(df.wim)
    count.vars <- grep( pattern=count.pattern,x=ic.names,perl=TRUE,value=TRUE)

    ic.names <- grep( pattern=count.pattern,
                     x=ic.names,perl=TRUE,value=TRUE,
                     invert=TRUE)

    mean.vars <- grep(pattern=mean.pattern,
                      x=ic.names,
                      perl=TRUE,value=TRUE)

    mean.vars <- grep(pattern=mean.exclude.pattern,
                      x=mean.vars,
                      perl=TRUE,value=TRUE,
                      invert=TRUE)

    M <- 10000000000  #arbitrary bignum for max limits

    ic.names <- names(df.wim)

    ## generate the position of each count variable
    pos.count <- (1:length(ic.names))[is.element(ic.names, c(count.vars))]

    ## for the count variables,
    ## make a max allowed value of 110% of observed values and a min value of 0
    max.vals <- apply( df.wim[,count.vars], 2, max ,na.rm=TRUE)
    min.vals <- apply( df.wim[,count.vars], 2, min ,na.rm=TRUE)
    pos.bds <- cbind(pos.count,min.vals,1.10*max.vals)


    ## for axles, we need to limit min of zero, max of observed?
    ## because it isn't per truck it is per sum of trucks
    pos.means <- (1:length(ic.names))[is.element(ic.names, c(mean.vars))]
    max.vals <- apply( df.wim[,mean.vars], 2, max ,na.rm=TRUE)
    min.vals <- apply( df.wim[,mean.vars], 2, min ,na.rm=TRUE)
    pos.bds <- rbind(pos.bds,cbind(pos.means,min.vals,max.vals))

    sqrt.vars <- c(count.vars,mean.vars)
    sqrt.vars <- grep(pattern='all_veh_speed',
                      x=sqrt.vars,
                      perl=TRUE,
                      value=TRUE,
                      invert=TRUE)

    exclude.as.id.vars <- setdiff(ic.names,c(mean.vars,count.vars,'tod','day'))

    df.truckamelia.b <-
        Amelia::amelia(df.wim,
                       idvars=exclude.as.id.vars,
                       ts="tod",
                       splinetime=6,
                       lags =count.vars,
                       leads=count.vars,
                       sqrts=sqrt.vars,
                       cs="day",intercs=TRUE,
                       emburn=c(2,20),
                       bounds = pos.bds,
                       max.resample=10
                       ## ,empri = 0.05 *nrow(df.wim)
                       )

    df.merged <- condense.amelia.output(df.truckamelia.b)

})


rcouchutils::couch.deletedb(parts)
