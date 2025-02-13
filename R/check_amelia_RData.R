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
##' @param check_times if TRUE, will also double check that times of
##'     VDS type data line up with the raw data
##' @return 0 if the file holds a good imputation result; 1 if the
##'     file does not hold a good imputation result; and 2 if the file
##'     is not an amelia object
##' @author James E. Marca
##' @export
##'
amelia_output_file_status <- function(file,db,check_times=FALSE){

    env <- new.env()
    res <- load(file=file,envir=env)

    if(class(x=env[[res]]) != "amelia"){
        return (2)
    }

    message <- env[[res]]$message

    ## print(message)
    match_it <- grep(pattern='^Normal EM',ignore.case=TRUE,perl=TRUE,x=message)
    if(match_it == 1){
        print('good result')
        ## good result
        if(length(env[[res]]$imputations) == 5){
           print('good imputations number')
           ## good number of imputations

            if(check_times){
                ## double check that time is not offset
                path_parts <- strsplit(file,split="/")[[1]]
                file_name <- path_parts[length(path_parts)]
                file_name_parts <- strsplit(file_name,"\\.")[[1]]
                fname <- file_name_parts[1]
                year <- strsplit(fname,"_")[[1]][3]
                seconds <- as.numeric(file_name_parts[2])
                ## fixup the path
                path_parts <- path_parts[-length(path_parts)]
                not_empty <- path_parts != ''

                path <- paste(path_parts[not_empty],sep='',collapse='/')
                if(! not_empty[1] ){
                    ## meaning, the first element of path_parts is ''
                    ## because it is an absolute path, need to put
                    ## back the leading slash
                    path <- paste('/',path,sep='')
                }
                path <- paste(path,'/',sep='')
                print('checking time offset')
                result <- detect_broken_imputed_time(fname=fname,
                                                     year=year,
                                                     path=path,
                                                     delete_it=FALSE,
                                                     seconds=seconds,
                                                     trackingdb=db)
                print(result)
                if(!result){
                    return (0)
                }
            }else{
                return(0)
            }

        }
    }

    ## if(!missing(db)){
    ##     result <- decode_amelia_output_file(f)
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
