library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# List all files
all_files <- list.files("Dataset/")
print(all_files)

# Extract file names without prefix and suffix
all_files <- gsub("hi_", "", all_files)
all_files <- gsub("_dataset.csv", "", all_files)
print(all_files)

# Using R loops to check data integrity

# Check number of rows and columns
all_files <- list.files("Dataset/")
for (variable in all_files) {
  this_filepath <- paste0("Dataset/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  number_of_rows <- nrow(this_file_contents)
  number_of_columns <- ncol(this_file_contents)
  
  print(paste0("The file: ", variable,
               " has: ",
               format(number_of_rows, big.mark = ","),
               " rows and ",
               number_of_columns, " columns"))
}

# Check the data structure
for (variable in all_files) {
  this_filepath <- paste0("Dataset/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  data_structure <- str(this_file_contents)
  
  print(paste0(data_structure,
               "The file: ", variable,
               " has above data structure"))
}

# Check for NULL values
for (variable in all_files) {
  this_filepath <- paste0("Dataset/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  null <- sum(is.na(this_file_contents))
  
  print(paste0("The file: ", variable,
               " has a total of ", null,
               " NULL values"))
}

# Check that each primary key is unique in each table except for order
for (variable in all_files) {
  this_filepath <- paste0("Dataset/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  hi <- nrow(unique(this_file_contents[, 1])) == nrow(this_file_contents)
  
  print(paste0("The file: ", variable,
               " has unique primary key ",
               hi, " columns"))
}

# For order dataset
orderdate_dataset <- read.csv("Dataset/hi_order_datetime_dataset.csv")
orderproductsinfo_dataset <- read.csv("Dataset/hi_order_products_info_dataset.csv")
print(nrow(unique(orderdate_dataset[, 1:2])) == nrow(orderdate_dataset))
print(nrow(unique(orderproductsinfo_dataset[, 1:3])) == nrow(orderproductsinfo_dataset))

# Load Files in an sqlite database 
connection <- RSQLite::dbConnect(RSQLite::SQLite(), "hi_import.db")

# Drop tables
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS product")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS product_category")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS promotion")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS supplier")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS 'transaction'")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS order_datetime")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS order_products_info")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS actual_delivery_date")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS delivery_tracking")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS estimated_delivery_date")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS delivery")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS customer_membership")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS customer_basic_info")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS customer")
RSQLite::dbExecute(connection, "DROP TABLE IF EXISTS 'order'")

# Create SQL tables

# product_category
RSQLite::dbExecute(connection, "
                   CREATE TABLE product_category (
                   category_name VARCHAR(50) PRIMARY KEY,
                   parent_category_id INT NULL
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM product_category;"))

# promotion
RSQLite::dbExecute(connection, "
                   CREATE TABLE promotion (
                   promo_code INT PRIMARY KEY, 
                   promo_start_date DATE NULL,
                   promo_expire_date DATE NULL,
                   percentage_discount NUMERIC NOT NULL
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM promotion;"))

# supplier
RSQLite::dbExecute(connection, "
                   CREATE TABLE supplier (
                   supplier_id INT PRIMARY KEY, 
                   supplier_name CHAR NOT NULL,
                   supplier_phone INT NOT NULL,
                   supplier_email VARCHAR(50) NOT NULL,
                   supplier_building INT NOT NULL,
                   supplier_street VARCHAR(50) NOT NULL,
                   supplier_city VARCHAR(50) NOT NULL,
                   supplier_postcode VARCHAR(50) NOT NULL
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM supplier;"))

# customer
RSQLite::dbExecute(connection, "
                   CREATE TABLE customer (
                   customer_id INT PRIMARY KEY, 
                   customer_firstname VARCHAR(50) NOT NULL,
                   customer_lastname VARCHAR(50) NOT NULL,
                   customer_title VARCHAR(25) NOT NULL, 
                   customer_phone VARCHAR(50) NOT NULL,
                   customer_email VARCHAR(50) NOT NULL,
                   customer_membership TEXT NOT NULL, 
                   delivery_fee NUMERIC NOT NULL,
                   customer_building INT NOT NULL,
                   customer_street VARCHAR(50) NOT NULL, 
                   customer_city VARCHAR(50) NOT NULL, 
                   customer_postcode VARCHAR(50) NOT NULL, 
                   promo_code INT,
                   FOREIGN KEY (promo_code) REFERENCES promotion(promo_code)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer;"))

# delivery
RSQLite::dbExecute(connection, "
                   CREATE TABLE delivery (
                   tracking_number INT PRIMARY KEY, 
                   shipment_method VARCHAR(50) NOT NULL,
                   tracking_status VARCHAR(50) NOT NULL,
                   estimated_delivery_date DATE NOT NULL,
                   estimated_delivery_time TIME NOT NULL,
                   actual_delivery_date DATE NULL, 
                   actual_delivery_time TIME NULL,
                   delivery_instructions VARCHAR(125) NOT NULL,
                   trans_id INT,
                   FOREIGN KEY (trans_id) REFERENCES 'transaction'(trans_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM delivery;"))

# product
RSQLite::dbExecute(connection, "
                   CREATE TABLE product (
                   product_id INT PRIMARY KEY, 
                   product_name VARCHAR(25) NOT NULL,
                   product_weight NUMERIC NOT NULL,
                   product_length NUMERIC NOT NULL,
                   product_height NUMERIC NOT NULL,
                   product_width NUMERIC NOT NULL,
                   product_price NUMERIC NOT NULL,
                   supplier_id INT,
                   category_name VARCHAR(50),
                   FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id),
                   FOREIGN KEY (category_name) REFERENCES product_category(category_name)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM product;"))

# order
RSQLite::dbExecute(connection, "
                   CREATE TABLE 'order' (
                   customer_id INT,
                   order_id INT,
                   product_id INT,
                   product_qty INT NOT NULL,
                   order_date DATE NOT NULL, 
                   order_time TIME NOT NULL,
                   PRIMARY KEY (customer_id, order_id, product_id),
                   FOREIGN KEY (customer_id) REFERENCES customer(customer_id),
                   FOREIGN KEY (product_id) REFERENCES product(product_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM 'order';"))

# transaction
RSQLite::dbExecute(connection, "
                   CREATE TABLE 'transaction' (
                   trans_id INT PRIMARY KEY,
                   order_id INT,
                   trans_date DATE NOT NULL,
                   trans_time TIME NOT NULL,
                   FOREIGN KEY (order_id) REFERENCES 'order'(order_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM 'transaction';"))

# Normalization to 3NF

# For customer
# customer_basic_info
RSQLite::dbExecute(connection, "
                   CREATE TABLE customer_basic_info (
                   customer_id INT PRIMARY KEY, 
                   customer_firstname VARCHAR(50) NOT NULL,
                   customer_lastname VARCHAR(50) NOT NULL,
                   customer_title VARCHAR(25) NOT NULL, 
                   customer_phone VARCHAR(50) NOT NULL,
                   customer_email VARCHAR(50) NOT NULL,
                   customer_building INT NOT NULL,
                   customer_street VARCHAR(50) NOT NULL, 
                   customer_city VARCHAR(50) NOT NULL, 
                   customer_postcode VARCHAR(50) NOT NULL, 
                   promo_code INT,
                   FOREIGN KEY (promo_code) REFERENCES promotion(promo_code)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer_basic_info;"))

# customer_membership
RSQLite::dbExecute(connection, "
                   CREATE TABLE customer_membership (
                   customer_id INT, 
                   customer_membership TEXT, 
                   delivery_fee NUMERIC NOT NULL,
                   PRIMARY KEY (customer_id, customer_membership),
                   FOREIGN KEY (customer_id) REFERENCES customer_basic_info(customer_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer_membership;"))

# For order
# order_products_info
RSQLite::dbExecute(connection, "
                   CREATE TABLE order_products_info (
                   customer_id INT,
                   order_id INT,
                   product_id INT,
                   product_qty INT NOT NULL,
                   PRIMARY KEY (customer_id, order_id, product_id),
                   FOREIGN KEY (customer_id) REFERENCES customer_basic_info(customer_id),
                   FOREIGN KEY (product_id) REFERENCES product(product_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_products_info;"))

# order_datetime
RSQLite::dbExecute(connection, "
                   CREATE TABLE order_datetime (
                   customer_id INT,
                   order_id INT,
                   order_date DATE NOT NULL, 
                   order_time TIME NOT NULL,
                   PRIMARY KEY (customer_id, order_id),
                   FOREIGN KEY (customer_id) REFERENCES customer_basic_info(customer_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_datetime;"))

# For Delivery
# delivery_tracking
RSQLite::dbExecute(connection, "
                   CREATE TABLE delivery_tracking (
                   tracking_number INT PRIMARY KEY, 
                   delivery_instructions VARCHAR(125) NOT NULL,
                   trans_id INT,
                   FOREIGN KEY (trans_id) REFERENCES 'transaction'(trans_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM delivery_tracking;"))

# estimated_delivery_date
RSQLite::dbExecute(connection, "
                   CREATE TABLE estimated_delivery_date (
                   tracking_number INT, 
                   shipment_method VARCHAR(50),
                   estimated_delivery_date DATE NOT NULL,
                   estimated_delivery_time TIME NOT NULL,
                   PRIMARY KEY (tracking_number, shipment_method),
                   FOREIGN KEY (tracking_number) REFERENCES delivery_tracking(tracking_number)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM estimated_delivery_date;"))

# actual_delivery_date
RSQLite::dbExecute(connection, "
                   CREATE TABLE actual_delivery_date (
                   tracking_number INT, 
                   tracking_status VARCHAR(50),
                   actual_delivery_date DATE NULL, 
                   actual_delivery_time TIME NULL,
                   PRIMARY KEY (tracking_number, tracking_status),
                   FOREIGN KEY (tracking_number) REFERENCES delivery_tracking(tracking_number)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM actual_delivery_date;"))

# Import csv files into SQL table
for (variable in all_files) {
  this_filepath <- paste0("Dataset/", variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  table_name <- gsub(".csv", "", variable)
  table_name <- gsub("hi_", "", table_name)
  table_name <- gsub("_dataset", "", table_name)
  
  RSQLite::dbWriteTable(connection, table_name, this_file_contents, append = TRUE, row.names = FALSE)
}

# Check the tables using select
print(RSQLite::dbGetQuery(connection, "SELECT * FROM product LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM product_category LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM promotion LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM supplier LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM 'transaction' LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM promotion LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_datetime LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_products_info LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM actual_delivery_date LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM delivery_tracking LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM estimated_delivery_date LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer_membership LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer_basic_info LIMIT 5"))



# Calculated Field: Transaction amount
print(RSQLite::dbGetQuery(connection, "
                          SELECT o.order_id, 
                                 prm.percentage_discount, 
                                 m.delivery_fee, 
                                 SUM(p.product_price*o.product_qty) AS order_price, 
                                 ROUND(SUM(p.product_price* o.product_qty)*(1 - CAST(prm.percentage_discount AS REAL) / 100) + m.delivery_fee,2) AS trans_amount
                          FROM order_products_info o, 
                               product p, 
                               promotion prm, 
                               customer_basic_info c, 
                               customer_membership m
                          WHERE o.customer_id = m.customer_id 
                                AND o.product_id = p.product_id 
                                AND c.promo_code = prm.promo_code 
                                AND m.customer_id = c.customer_id
                          GROUP BY o.order_id;"))

# Store natively it in R
customer_membership <- dbReadTable(connection, "customer_membership")
customer_basic_info <- dbReadTable(connection, "customer_basic_info")
actual_delivery_date <- dbReadTable(connection, "actual_delivery_date")
delivery_tracking <- dbReadTable(connection, "delivery_tracking")
estimated_delivery_date <- dbReadTable(connection, "estimated_delivery_date")
order_datetime <- dbReadTable(connection, "order_datetime")
order_products_info <- dbReadTable(connection, "order_products_info")
product <- dbReadTable(connection, "product")
product_category <- dbReadTable(connection, "product_category")
promotion <- dbReadTable(connection, "promotion")
supplier <- dbReadTable(connection, "supplier")
transaction <- dbReadTable(connection, "transaction")

# List tables
print(RSQLite::dbListTables(connection))


# Disconnect SQL
RSQLite::dbDisconnect(connection)
