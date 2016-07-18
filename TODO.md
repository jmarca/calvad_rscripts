# TODO

## July 2016

Input of raw VDS data is fragile.

Reading raw data with perl saving to file, can sometimes crash, and
they you're messed up.

A few fixes in R can help.  When reading raw data, try piping through
sqldf to remove duplicate entries.  Also be more robust in the read
command.
