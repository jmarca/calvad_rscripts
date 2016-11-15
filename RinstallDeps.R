## need node_modules directories
dot_is <- getwd()
node_paths <- dir(dot_is,pattern='\\.Rlibs',
                  full.names=TRUE,recursive=TRUE,
                  ignore.case=TRUE,include.dirs=TRUE,
                  all.files = TRUE)
path <- normalizePath(paste(dot_is,'.Rlibs',sep='/')
                    , winslash = "/", mustWork = FALSE)
if(!file.exists('path')){
    dir.create(path)
}
lib_paths <- .libPaths()
.libPaths(c(path,node_paths,lib_paths))

install.packages("roxygen2",repos="https://cloud.r-project.org")
devtools::install_deps()
