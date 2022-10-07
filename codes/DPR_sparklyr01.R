# Data Programming with R
# Demonstration usnig Spark in R and 
# Automated Machine Learning (AML) with h2o
install.packages("sparklyr")
library(sparklyr)

# install Spark on your local computer, treating it like a cluster
# internet connection required

spark_install()

# Connect to cluster
sc <- spark_connect(master = "local")

# Check Spark connections and environment

# Copy data to Spark session's memory
tbl_teds16 <- copy_to(sc, TEDS2016, "spark_teds2016")
class(tbl_teds16)

# Alternative method to load local csv to Spark
spark_read_csv(sc, name = "teds16",  path = "/path/TEDS2016.csv")

# Disconnect
spark_disconnect(sc)

sdf_describe(votetsai, cols = colnames(votetsai))

partitions <- tbl_teds16 %>%
  select(votetsai, dpp, kmt, unify, statusquo, female) %>% 
  sdf_random_split(training = 0.5, test = 0.5, seed = 1099)


fit <- partitions$training %>%
  ml_logistic_regression(votetsai ~ .)
summary(fit)

pred <- ml_predict(fit, partitions$test)

## Use Spark and H2O
### https://docs.h2o.ai/sparkling-water/3.3/latest-stable/doc/rsparkling.html

if ("package:h2o" %in% search()) { detach("package:h2o", unload=TRUE) }
if ("h2o" %in% rownames(installed.packages())) { remove.packages("h2o") }

# Install packages H2O depends on
pkgs <- c("methods", "statmod", "stats", "graphics", "RCurl", "jsonlite", "tools", "utils")
for (pkg in pkgs) {
  if (! (pkg %in% rownames(installed.packages()))) { install.packages(pkg) }
}
install.packages("h2o", "3.38.0.1")  
# Download, install, and initialize the RSparkling
install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.3/3.38.0.1-1-3.3/R")

library(rsparkling)

sc <- spark_connect(master = "local", version = "3.3.0")
library(h2o)
#install.packages("rsparkling")
library(rsparkling)
library(sparklyr)
library(dplyr)

h2oConf <- H2OConf()
hc <- H2OContext.getOrCreate(h2oConf)
hc$openFlow()
vote_h2o <- hc$asH2OFrame(tbl_teds16)


vote_glm <- h2o.glm(x = c("dpp", "female"),
                      y = "votetsai",
                      training_frame = vote_h2o,
                      lambda_search = TRUE)
vote_glm
