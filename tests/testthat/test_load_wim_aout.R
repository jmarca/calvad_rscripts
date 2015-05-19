config <- rcouchutils::get.config(Sys.getenv('RCOUCHUTILS_TEST_CONFIG'))
parts <- c('wim','amelia','trials')
rcouchutils::couch.makedb(parts)

test_that(
    "can find amelia output",{

        site_no <- 37
        direction <- 'S'
        year <- 2012

        my_df <- get.amelia.wim.file.local(site_no=site_no,
                                           direction=direction,
                                           path='./files',
                                           year=2012)
        expect_that(my_df,is_a('amelia'))


    })


test_that(
    "can load wim amelia output",{

        file  <- './files/2012/37/S/wim37S.3600.imputed.RData'
        site_no <- 37
        direction <- 'S'
        year <- 2012
        seconds <- 120
        path <- './files/2012/37/S'
        env <- new.env()
        res <- load(file=file,envir = env)
        aout <- env[[res]]

        df.merged <- condense.amelia.output(aout)
        all_the_hours <- plyr::count(df.merged$ts)
        for(i in 1:length(all_the_hours)){
            expect_that(all_the_hours[i,2],equals(1))
        }
        hour_counts <- plyr::count(df.merged$tod)
        for(hour in 1:24){
            expect_that(hour_counts[[hour,2]],equals(366))
        }


})


rcouchutils::couch.deletedb(parts)
