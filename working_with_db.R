# Load the package required to read JSON files.
library(DBI)
library(dbplyr)
library(duckdb)
library(tidyverse)

# this connection connects to an existing database, yelp.duckdb
con <- DBI::dbConnect(duckdb::duckdb(dbdir = "coffee_db"))

# View database tables
DBI::dbListTables(con)

# view data from table
con |> 
  DBI::dbReadTable("employees")

# view table as a tibble in R
con |> 
  DBI::dbReadTable("employees") |> 
  as_tibble()

# using SQL queries
sql <- "
  SELECT * 
  FROM employees
"

as_tibble(dbGetQuery(con, sql))

# database table object
employees_table_object <- dplyr::tbl(con, "employees")
employees_table_object

# Another example
dplyr::tbl(con, "employees") |> 
  filter(between(salary , 35000, 50000)) |> 
  select(employee_id,first_name,last_name,salary)

# Generate SQL code
dplyr::tbl(con, "employees") |> 
  filter(between(salary , 35000, 50000)) |> 
  select(employee_id,first_name,last_name,salary) |> 
  dplyr::show_query()


dplyr::tbl(con, "employees") |> 
  filter(between(salary , 35000, 50000)) |> 
  select(employee_id,first_name,last_name,salary) |> 
  dplyr::collect()

employees_with_salary_between_35k_and_50k <-
  dplyr::tbl(con, "employees") |>
  filter(between(salary , 35000, 50000)) |>
  select(employee_id, first_name, last_name, salary) |>
  dplyr::collect()



