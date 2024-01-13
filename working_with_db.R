# Load the package required to read JSON files.
library(DBI)
library(dbplyr)
library(duckdb)
library(tidyverse)

# this connection connects to an existing database, yelp.duckdb
con <- DBI::dbConnect(duckdb::duckdb(dbdir = "coffee_db"))

# View database tables
DBI::dbListTables(con)

# connect and view table as a tibble in R
con |> 
  DBI::dbReadTable("employees") |> 
  as_tibble()