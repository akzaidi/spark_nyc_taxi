# Update CRAN Mirror ------------------------------------------------------

r <- getOption("repos")
mran_date <- Sys.Date() - 1
r[["CRAN"]] <- paste0("https://mran.revolutionanalytics.com/snapshot/", mran_date)
options(repos = r)


# Install Dependencies ----------------------------------------------------

# system("sudo apt-get update")
# system("sudo apt-get install libgeos-dev -y")

# Install Packages --------------------------------------------------------

reqd_pkgs <- c('dplyr', 'stringr', 'lubridate', 
                 'rgeos', 'sp', 'maptools', 
               'ggmap', 'ggplot2', 'gridExt', 
               'ggrepel', 'tidyr', 'seriation')

install.packages(reqd_pkgs)

# install.packages('dplyr')
# install.packages('stringr')
# install.packages('lubridate')
# install.packages('rgeos') # spatial package
# install.packages('sp') # spatial package
# install.packages('maptools') # spatial package
# install.packages('ggmap')
# install.packages('ggplot2')
# install.packages('gridExtra') # for putting plots side by side
# install.packages('ggrepel') # avoid text overlap in plots
# install.packages('tidyr')
# install.packages('seriation') # package for reordering a distance matrix


# Check Versions ----------------------------------------------------------

lapply(reqd_pkgs, packageVersion)
