
# Load the package required to read JSON files.
library(DBI)
library(dbplyr)
library(duckdb)
library(tidyverse)

# this connection connects to an existing database, yelp.duckdb
con <- DBI::dbConnect(duckdb::duckdb(dbdir = "coffee_db"))

# extract required data
employees_with_salary_between_35k_and_50k <-
  dplyr::tbl(con, "employees") |>
  filter(between(salary , 35000, 50000)) |>
  select(employee_id, first_name, last_name, salary) |>
  dplyr::collect()