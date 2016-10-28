
# Update CRAN Mirror ------------------------------------------------------

r <- getOption("repos")
r[["CRAN"]] <- "https://mran.revolutionanalytics.com/snapshot/2016-10-25"
options(repos = r)

# Download Data -----------------------------------------------------------

system("chmod +x ./download_taxi.sh")
system("sudo ./download_taxi.sh")


# Move to HDFS ------------------------------------------------------------

taxi_files <- list.files("data/")
lapply(taxi_files, function(x) {
  rxHadoopCopyFromLocal(source = paste0("data/", x), 
                        dest = paste0("/user/RevoShare/alizaidi/taxidata/", x))
})


# Create Pointers to HDFS -------------------------------------------------

myNameNode <- "default"
myPort <- 0
hdfsFS <- RxHdfsFileSystem(hostName = myNameNode, 
                           port = myPort)


taxi_path <- file.path("/user/RevoShare/alizaidi",
                       "taxidata/")

taxi_dir <- RxTextData(taxi_path,
                        fileSystem = hdfsFS)



taxi_xdf <- RxXdfData(file.path("/user/RevoShare/alizaidi",
                                "taxidataXdf"),
                      fileSystem = hdfsFS)
                      


# Create Spark Compute Context --------------------------------------------

spark_cc <- RxSpark(consoleOutput = TRUE,
                    nameNode = myNameNode,
                    port = myPort,
                    executorCores = 12, 
                    executorMem = "10g", 
                    executorOverheadMem = "5g", 
                    persistentRun = TRUE, 
                    extraSparkConfig = "--conf spark.speculation=true")
                          

rxSetComputeContext(spark_cc)



# Import to XDF -----------------------------------------------------------

system.time(rxImport(inData = taxi_dir,
                     outFile = taxi_xdf))
