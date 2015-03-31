##' Create truck mean values of various sorts
##'
##'
##' @title create.truck.means
##' @param df.wim
##' @return a new dataframe with averages for hh,nhh
##' @author James E. Marca
create.truck.means <- function(df.wim){
    ## nh goes with not_heavyheavy
    ## hh goes with heavyheavy
    var.names <- names(df.wim)
    heavy.heavy.vars <- grep(pattern="^heavyheavy"
                            ,x=var.names
                            ,perl=TRUE
                            ,value=TRUE)
    left.lane <- grep(pattern="l1"
                 ,x=heavy.heavy.vars
                 ,perl=TRUE
                 ,value=TRUE)

    num.lanes <- length(heavy.heavy.vars)
    lanes <- c()

    if(length(left.lane)>0){
        num.lanes <- num.lanes - 1
        lanes <- c('l1')
    }
    for(l in 1:num.lanes){
        lanes <- c(lanes,paste('r',l,sep=''))
    }

    not.heavy.heavy.vars <- grep(pattern="^not_heavyheavy"
                                ,x=var.names
                                ,perl=TRUE
                                ,value=TRUE)

    hh.vars <- grep(pattern="^hh"
                   ,x=var.names
                   ,perl=TRUE
                   ,value=TRUE)
    nh.vars <- grep(pattern="^nh"
                   ,x=var.names
                   ,perl=TRUE
                   ,value=TRUE)
    df.ave <- data.frame(ts=df.wim$ts)

    for(l in lanes){
        ## hh first
        hhcount <- grep(pattern=paste(l,"$",sep='')
                       ,x=heavy.heavy.vars
                       ,perl=TRUE
                       ,value=TRUE)
        hh.aves <- grep(pattern=paste(l,"$",sep='')
                       ,x=hh.vars
                       ,perl=TRUE
                       ,value=TRUE)
        df.ave[,hh.aves] <- df.wim[,hh.aves]/df.wim[,hhcount]

        nhcount <- grep(pattern=paste(l,"$",sep='')
                       ,x=not.heavy.heavy.vars
                       ,perl=TRUE
                       ,value=TRUE)
        nh.aves <- grep(pattern=paste(l,"$",sep='')
                       ,x=nh.vars
                       ,perl=TRUE
                       ,value=TRUE)
        df.ave[,nh.aves] <- df.wim[,nh.aves]/df.wim[,nhcount]

    }
    df.ave
}
