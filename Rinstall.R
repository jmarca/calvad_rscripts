envrr <- Sys.getenv()
dot_is <- getwd()

plaba <- regexpr(pattern='/node_modules',envrr['PWD'])
if(plaba>0){
    ## stoppoint <- plaba + 12 ## same as  attr(plaba,'match.length')
    stoppoint <- plaba  ## actually, go one up
    dot_is <- substr(envrr['PWD'],1,stoppoint)
}
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(paste(dot_is,'.Rlibs',sep='/')
                    , winslash = "/", mustWork = FALSE)
if(!file.exists(path)){
    dir.create(path)
}

lib_paths <- .libPaths()
.libPaths(c(path,node_paths,lib_paths))

## ready to go
devtools::document()
devtools::install()
