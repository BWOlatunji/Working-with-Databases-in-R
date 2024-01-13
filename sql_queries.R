# Load the package required to read JSON files.
library(DBI)
library(dbplyr)
library(duckdb)
library(tidyverse)

# this connection connects to an existing database, yelp.duckdb
con <- DBI::dbConnect(duckdb::duckdb(dbdir = "coffee_db"))


# access data from duckdb database tables
# Remember these are not actual data frame and are not available in the RStudio environment
dplyr::tbl(con, "employees") 
dplyr::tbl(con, "shops") 
dplyr::tbl(con, "locations") 
dplyr::tbl(con, "suppliers") 

# to get the data into RStudio environment, 
# you will need to convert it to a tibble and assign it a varaiable name
employees <- dplyr::tbl(con, "employees") |> as_tibble()

# SELECT statement

# select all (*) columns from table
dplyr::tbl(con, "employees") |> show_query()
dplyr::tbl(con, "shops") |> show_query()
dplyr::tbl(con, "locations") |> show_query()
dplyr::tbl(con, "suppliers") |> show_query()


# select some (3) columns of table

dplyr::tbl(con, "employees") |> dplyr::select(employee_id,
                                              first_name,
                                              last_name)

dplyr::tbl(con, "employees") |>
  dplyr::select(employee_id,
                first_name,
                last_name) |> 
  show_query()

########################################################################

# WHERE clause + AND & OR

# Select only the employees who make more than 50k
dplyr::tbl(con, "employees") |>
  filter(salary > 50000)

# show SQL 
dplyr::tbl(con, "employees") |>
  filter(salary > 50000) |> 
  show_query()


# Select only the employees who work in Common Grounds coffeshop
dplyr::tbl(con, "employees") |>
  filter(coffeeshop_id == 1)

dplyr::tbl(con, "employees") |>
  filter(coffeeshop_id == 1) |> 
  show_query()


# Select all the employees who work in Common Grounds, make more than 50k and are male
dplyr::tbl(con, "employees") |>
  filter(salary> 50000 & coffeeshop_id == 1 & gender == 'M')


dplyr::tbl(con, "employees") |>
  filter(salary> 50000 & coffeeshop_id == 1 & gender == 'M') |> 
  show_query()



# Select all the employees who work in Common Grounds or make more than 50k or are male
dplyr::tbl(con, "employees") |>
  filter(salary> 50000 | coffeeshop_id == 1 | gender == 'M')


dplyr::tbl(con, "employees") |>
  filter(salary> 50000 | coffeeshop_id == 1 | gender == 'M') |> 
  show_query()


# IN, NOT IN, IS NULL, BETWEEN

# Select all rows from the suppliers table where the supplier is NOT Beans and Barley

dplyr::tbl(con, "suppliers") |> 
  filter(!(supplier_name == 'Beans and Barley'))

dplyr::tbl(con, "suppliers") |> 
  filter(!(supplier_name == 'Beans and Barley')) |> 
  show_query()

# Select all Robusta and Arabica coffee types
dplyr::tbl(con, "suppliers") |> 
  filter(coffee_type %in% c('Robusta', 'Arabica'))

dplyr::tbl(con, "suppliers") |> 
  filter(coffee_type %in% c('Robusta', 'Arabica')) |> 
  show_query()

dplyr::tbl(con, "suppliers") |> 
  filter(coffee_type == 'Robusta' | coffee_type == 'Arabica') |> 
  show_query()



# Select all coffee types that are not Robusta or Arabica

dplyr::tbl(con, "suppliers") |> 
  filter(!coffee_type %in% c('Robusta', 'Arabica'))

dplyr::tbl(con, "suppliers") |> 
  filter(!coffee_type %in% c('Robusta', 'Arabica')) |> 
  show_query()


# Select all employees with missing email addresses
dplyr::tbl(con, "employees") |> 
  filter(is.na(email))

dplyr::tbl(con, "employees") |> 
  filter(is.na(email)) |> 
  show_query()


# Select all employees whose emails are not missing
dplyr::tbl(con, "employees") |> 
  filter(!is.na(email))

dplyr::tbl(con, "employees") |> 
  filter(!is.na(email)) |> 
  show_query()

# Select all employees who make between 35k and 50k
dplyr::tbl(con, "employees") |> 
  filter(between(salary , 35000, 50000)) |> 
  select(employee_id,first_name,last_name,salary)

dplyr::tbl(con, "employees") |> 
  filter(between(salary , 35000, 50000)) |> 
  select(employee_id,first_name,last_name,salary) |> 
  show_query()


dplyr::tbl(con, "employees") |> 
  filter(salary>=35000 & salary <= 50000) |> 
  select(employee_id,first_name,last_name,salary) |> 
  show_query()



# ===========================================================

# ORDER BY, LIMIT, DISTINCT, Renaming columns

# Order by salary ascending 
dplyr::tbl(con, "employees") |>
  select(employee_id,
         first_name,
         last_name,
         salary) |> arrange(salary)

dplyr::tbl(con, "employees") |>
  select(employee_id,
         first_name,
         last_name,
         salary) |> arrange(salary) |> 
  show_query()

# Order by salary descending 
dplyr::tbl(con, "employees") |>
  select(employee_id,
         first_name,
         last_name,
         salary) |> arrange(desc(salary))

dplyr::tbl(con, "employees") |>
  select(employee_id,
         first_name,
         last_name,
         salary) |> arrange(desc(salary)) |> 
  show_query()

# Top 10 highest paid employees
dplyr::tbl(con, "employees") |>
  select(employee_id, first_name, last_name, salary) |>
  arrange(desc(salary)) |> 
  head(10)

dplyr::tbl(con, "employees") |>
  select(employee_id, first_name, last_name, salary) |>
  arrange(desc(salary)) |> 
  head(10) |> show_query()



# Return all unique coffeeshop ids
dplyr::tbl(con, "employees") |> 
  distinct(coffeeshop_id)


# Renaming columns
dplyr::tbl(con, "employees") |> 
  select(
    email,
    email_address = email,
    hire_date,
    date_joined = hire_date,
    salary,
    pay = salary
  )

dplyr::tbl(con, "employees") |> 
  select(
    email,
    email_address = email,
    hire_date,
    date_joined = hire_date,
    salary,
    pay = salary
  ) |> show_query()



# =========================================================

# EXTRACT
dplyr::tbl(con, "employees") |> 
  mutate(
    date = hire_date,
    year = year(hire_date),
    month = month(hire_date),
    day = day(hire_date)
  )  |> 
  select(date, year, month, day)


#=========================================================

# UPPER, LOWER, LENGTH, TRIM

# Uppercase first and last names
dplyr::tbl(con,"employees") |> 
  mutate(first_name_upper = str_to_upper(first_name),
         last_name_upper = str_to_upper(last_name)) |> 
  select(
    first_name,
    first_name_upper,
    last_name,
    last_name_upper
  ) |> show_query()




# Lowercase first and last names
dplyr::tbl(con,"employees") |> 
  mutate(first_name_upper = str_to_lower(first_name),
         last_name_upper = str_to_lower(last_name)) |> 
  select(
    first_name,
    first_name_upper,
    last_name,
    last_name_upper
  ) |> show_query()

# Return the email and the length of emails
dplyr::tbl(con, "employees") |> 
  mutate(email_length = str_length(email)) |> 
  select(email, email_length) |> show_query()



# TRIM

c("hello_with_spaces" = str_length('     HELLO     '),
  "hello_no_spaces" = str_length('HELLO'),
  "hello_trimmed" = str_length(str_trim('     HELLO     ')))

# =========================================================

# Concatenation, Boolean expressions, wildcards

# Concatenate first and last names to create full names
dplyr::tbl(con, "employees") |>
  select(first_name, last_name) |> 
  mutate(full_name = paste(first_name, last_name, sep = ' ')) |> 
  show_query()

# Concatenate columns to create a sentence
dplyr::tbl(con, "employees") |> 
  mutate(full_name_salary = paste(first_name, last_name, "makes", salary)) |> 
  select(full_name_salary) |> 
  show_query()


# Boolean expressios
# if the person makes less than 50k, then true, otherwise false
dplyr::tbl(con, "employees") |> 
  mutate(
    full_name = paste(first_name, last_name, sep = ' '),
    less_than_50k = salary < 50000
  ) |>
  select(full_name, less_than_50k)

