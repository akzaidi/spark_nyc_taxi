# NYC Taxi Examples for MRS on HDI

This repo contains scripts for running RevoScaleR functions using RxSpark on the NYC Taxi Data.

## Download

You will need the data to run these examples. Please run the download_taxi.sh script to download the data.

You can also follow the instructions in the 1-ingest.R module.


## Package Dependencies

You'll also need to install some packages. The required packages are in the pkg_requirements.R.R script, which you can run using `Rscript pkg_requirements.R`. This will install the packages on the edge node. To install them across your cluster, follow the instructions [here](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-r-server-get-started/#install-r-packages).
