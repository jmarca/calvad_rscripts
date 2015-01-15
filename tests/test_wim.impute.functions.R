source('../lib/wim.impute.functions.R',chdir=TRUE)
source('../lib/paired.Rdata.R',chdir=TRUE)

library(testthat)

test_that("fixup.plotfile.name will work properly", {

    filename <- ""
    expect_that(fixup.plotfile.name(filename),equals("_%03d.png"))

    filename <- "file"
    expect_that(fixup.plotfile.name(filename)
               ,equals(paste(filename,"_%03d.png",sep="")))


    filename <- "the/file"
    expect_that(fixup.plotfile.name(filename)
               ,equals(paste(filename,"_%03d.png",sep="")))

    filename <- "the/file.png"
    expect_that(fixup.plotfile.name(filename)
               ,equals("the/file_%03d.png"))


    filename <- "the/file_%03d.png"
    expect_that(fixup.plotfile.name(filename)
               ,equals(filename))

})
