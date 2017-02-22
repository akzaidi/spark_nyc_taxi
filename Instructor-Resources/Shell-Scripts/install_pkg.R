#!/usr/bin/env Rscript
options(repos = "https://cloud.r-project.org/")

args <- commandArgs(TRUE)
res <- try(install.packages(args))
if(inherits(res, "try-error")) q(status=1) else q()

