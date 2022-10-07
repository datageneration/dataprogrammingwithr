# Package management
## Note: this program demonstrates different methods of managing packages in R
## sessions, including installing packages and loading them in library.
## Some useful hotkeys: 
## Restarting R: Ctrl+Shift+F10/Cmd+Shift+0 = unload packages + clear objects
## (provided that in preference, Save workspace to .RData option is unchecked)
## gc() free up memrory and report the memory usage.
## rm(ls()) clear objects manually


### install multiple packages using c() combine function
install.packages(c("tidyverse", "data.table"))

ptbu=c("tidyverse","data.table")
install.packages(ptbu)
lapply(ptbu, require, character.only = TRUE)

### Check and install using Boolean logic
### Method 1

if(!require(ptbu)){
  install.packages(ptbu) 
  lapply(ptbu, require, character.only = TRUE)
  }

### Method 2

doInstall <- TRUE  # For checking if package is installed
toInstall <- c("tidyverse", "RColorBrewer", "GGally")
if(doInstall){install.packages(toInstall, repos = "http://cran.us.r-project.org")}
lapply(toInstall, require, character.only = TRUE) # call into library


### Method 3: Using "easypackages" pacakge

install.packages(c("easypackages","MASS","ISLR","arm"))
library(easypackages)
libraries("arm","MASS","ISLR")

### Check path of package
find.package("tidyverse") 

### Overall maintenance
update.packages(ask = FALSE) # without prompting package by package
