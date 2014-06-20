source('../lib/wim.impute.functions.R',chdir=TRUE)
source('../lib/paired.Rdata.R',chdir=TRUE)

library(testthat)

## must run this somewhere you can load vds data
test_that("fixup.plotfile.name will work properly", {

    filename <- ""
    expect_that(fixup.plotfile.name(filename),equals(filename))

    filename <- "file"
    expect_that(fixup.plotfile.name(filename)
               ,equals(paste(filename,"%03d.png",sep="")))


    filename <- "the/file"
    expect_that(fixup.plotfile.name(filename)
               ,equals(paste(filename,"%03d.png",sep="")))

    filename <- "the/file.png"
    expect_that(fixup.plotfile.name(filename)
               ,equals("the/file%03.png"))


    filename <- "the/file_%03d.png"
    expect_that(fixup.plotfile.name(filename)
               ,equals(filename))

}
