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
##' @param wim.path the root path in the file system to start looking
##' for the RData files.  The code will add in the year, then the site
##' number, then the direction, so that the file can be found in the
##' expected place
##' @param filename.pattern the pattern to use when searching.  Will
##' look for wim.agg.RData by default, but if, say, you want to load
##' the imputation output file, then pass in "imputed.RData" or
##' similar
##' @return a dataframe with the raw, unimputed data, or NULL
##' @author James E. Marca
get.wim.rdata <- function(wim.site,year,direction,
                          wim.path='/data/backup/wim'
                          ,filename.pattern='wim.agg.RData'){
    ## reload the saved, pre-imputation wim data
    search.path <- paste(wim.path,year,wim.site,direction,sep='/')
    isa.df <- dir(search.path, pattern=filename.pattern,
                  full.names=TRUE, ignore.case=TRUE,recursive=TRUE)

    if(length(isa.df)==0){
        return('todo')
    }

    env <- new.env()
    res <- load(file=isa.df,env)
    result <- list()
    result[[res]]=env

    return(result[[1]][[res]])

}
