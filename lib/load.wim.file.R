
get.wim.rdata <- function(wim.site,year,direction,wim.path='/data/backup/wim'){
    ## reload the saved, pre-imputation wim data

    target.file <- paste(wim.path,year,wim.site,direction,'wim.agg.RData',sep='/')
    print(paste('loading',target.file))
    load.result <- load(file=target.file)
    print(paste('load result is',load.result))
    if(load.result != 'local.df.wim.agg'){
        print(paste("choked loading?",df.wim.amelia))
        return(1)
    }
    local.df.wim.agg
}

get.wim.imputed <- function(wim.site,year,direction,wim.path='/data/backup/wim'){
    ## reload the imputed wim data
    cdb.wimid <- paste('wim',wim.site,direction,sep='.')
    savepath <- paste(wim.path,year,wim.site,direction,sep='/')
    target.file <- make.amelia.output.file(savepath,paste('wim',wim.site,direction,sep=''),seconds,year)
    print(paste('loading',target.file))
    load.result <- load(file=target.file)
    print(paste('load result is',load.result))
    if(length(df.wim.amelia) == 1){
        print(paste("amelia run for wim not good",df.wim.amelia))
        return(1)
    }else if(!length(df.wim.amelia)>0 || !length(df.wim.amelia$imputations)>0 || df.wim.amelia$code!=1 ){
        print("amelia run for vds not good")
        return(1)
    }
    ## use zoo to combine a mean value
    df.wim.amelia.c <- df.wim.amelia$imputations[[1]]
    for(i in 2:length(df.wim.amelia$imputations)){
        df.wim.amelia.c <- rbind(df.wim.amelia.c,df.wim.amelia$imputations[[i]])
    }
    print('median of amelia imputations')
    df.zoo <- medianed.aggregate.df(df.wim.amelia.c)
    rm(df.wim.amelia.c,df.wim.amelia)
    df.zoo
}
