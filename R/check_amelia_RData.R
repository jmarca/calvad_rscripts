##' Load and check a file that is supposed to hold amelia output
##'
##' In generating the imputations at various steps, The amelia output
##' is saved to RData files in the file system.  Sometimes these files
##' are "empty", containing just the details of a failed imputation
##' attempt.
##'
##' The fact that the files are not really successful imputations
##' means that if you change the imputation steps, then those failed
##' imputations should be re-run.  In order to determine which are
##' good and which are candidates for redos, you can use this
##' function.  It will load the file and return 0 if it is a good
##' amelia result, 1 if it is not a good amelia result, and 2 if the
##' passed in filename does not contain an object of class amelia
##'
##' The definition of "good result" is that the loaded object
##'
##' 1. an object of class amelia
##' 2. has a $message that matches "^Normal EM"
##' 3. has 5 $imputations
##'
##' @title amelia_output_file_status
##' @param file a full filename to load for processing
##' @param db a couchdb database to save state.  Optional and unused
##'     at the moment
##' @return 0 if the file holds a good imputation result; 1 if the
##'     file does not hold a good imputation result; and 2 if the file
##'     is not an amelia object
##' @author James E. Marca
##' @export
##'
amelia_output_file_status <- function(file,db){

    env <- new.env()
    res <- load(file=file,envir=env)

    if(class(x=env[[res]]) != "amelia"){
        return (2)
    }

    message <- env[[res]]$message

    ## print(message)
    match_it <- grep(pattern='^Normal EM',ignore.case=TRUE,perl=TRUE,x=message)
    if(match_it == 1){
        ## good result
        if(length(env[[res]]$imputations) == 5){
            ## good number of imputations
            return (0)
        }
    }

    ## if(!missing(db)){
    ##     result <- decode_amelia_output_file(file)
    ##     docid <- NULL
    ##     if(is.null(result$site_no)){
    ##         ## vds site
    ##         docid <- result$vds_id[1]
    ##     }else{
    ##         docid <- paste('wim',result$site_no[1],result$direction[1],sep='/')
    ##     }
    ##     rcouchutils::couch.set.state(
    ##         year=result$year[1],id=docid,db=db
    ##         doc=list(
    ##             'raw_imputation_code'=env[[res]]$code,
    ##             'raw_imputation_message'=env[[res]]$message
    ##         )
    ##     )
    ## }
    return (1)

}
