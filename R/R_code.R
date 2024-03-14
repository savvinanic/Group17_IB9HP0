library(readr)
library(RSQLite)

all_files <- list.files("data_upload/")
all_files

prefix <- "hi_"
suffix <- "_dataset.csv"

all_files <- gsub("hi_","",all_files)
all_files <- gsub("_dataset.csv","",all_files)
all_files

connection <- RSQLite::dbConnect(RSQLite::SQLite(),"hi_import.db")