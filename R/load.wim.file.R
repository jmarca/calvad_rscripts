##' Get WIM dataframe from a saved RData file
##'
##' I stopped using this at one point when faced with borked RData
##' files.  Probably safe to start using again.
##'
##' Given the WIM site number, the year, and the direction, will get
##' the correct RData file from some directory under the path
##' parameter.  If it exists, will return the reconstituted dataframe.
##' If not, not.
##'
##' @title get.wim.rdata
##' @param wim.site the WIM site number
##' @param year the year
##' @param direction the direction
##' @param wim.path the path in the file system to start looking for
##' the RData files
##' @return a dataframe with the raw, unimputed data, or NULL
##' @author James E. Marca
get.wim.rdata <- function(wim.site,year,direction,wim.path='/data/backup/wim'){
    ## reload the saved, pre-imputation wim data

    target.file <- paste(wim.path,year,wim.site,direction,'wim.agg.RData',sep='/')
    print(paste('loading',target.file))

    env <- new.env()
    res <- load(file=target.file,env)
    result <- list()
    result[[res]]=env
    print(paste('load result is',res))
    if(res != 'local.df.wim.agg'){
        print(paste("choked loading?",res,"is not local.df.wim.agg as expected"))
        return(NULL)
    }
    return(result[[1]][[res]])

}
