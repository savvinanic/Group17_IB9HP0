library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Load Files in an sqlite database 
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"database/hi_import.db")

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
                   parent_category_id CHAR NULL
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
                   promo_code INT,
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
                   FOREIGN KEY (promo_code) REFERENCES promotion(promo_code)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM customer;"))


# product
RSQLite::dbExecute(connection, "
                   CREATE TABLE product (
                   product_id INT PRIMARY KEY, 
                   supplier_id INT,
                   category_name VARCHAR(50),
                   product_name VARCHAR(25) NOT NULL,
                   product_weight NUMERIC NOT NULL,
                   product_length NUMERIC NOT NULL,
                   product_height NUMERIC NOT NULL,
                   product_width NUMERIC NOT NULL,
                   product_price NUMERIC NOT NULL,
                   FOREIGN KEY (supplier_id) REFERENCES supplier(supplier_id),
                   FOREIGN KEY (category_name) REFERENCES product_category(category_name)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM product;"))

# order
RSQLite::dbExecute(connection, "
                   CREATE TABLE 'order' (
                   order_id INT,
                   customer_id INT,
                   product_id INT,
                   product_qty INT NOT NULL,
                   order_date DATE NOT NULL, 
                   order_time TIME NOT NULL,
                   PRIMARY KEY (order_id, customer_id, product_id),
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

# delivery
RSQLite::dbExecute(connection, "
                   CREATE TABLE delivery (
                   tracking_number INT PRIMARY KEY, 
                   trans_id INT,
                   shipment_method VARCHAR(50) NOT NULL,
                   tracking_status VARCHAR(50) NOT NULL,
                   estimated_delivery_date DATE NOT NULL,
                   estimated_delivery_time TIME NOT NULL,
                   actual_delivery_date DATE NULL, 
                   actual_delivery_time TIME NULL,
                   delivery_instructions VARCHAR(125) NOT NULL,
                   FOREIGN KEY (trans_id) REFERENCES 'transaction'(trans_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM delivery;"))

# Normalization to 3NF

# For customer
# customer_basic_info
RSQLite::dbExecute(connection, "
                   CREATE TABLE customer_basic_info (
                   customer_id INT PRIMARY KEY, 
                   promo_code INT,
                   customer_firstname VARCHAR(50) NOT NULL,
                   customer_lastname VARCHAR(50) NOT NULL,
                   customer_title VARCHAR(25) NOT NULL, 
                   customer_phone VARCHAR(50) NOT NULL,
                   customer_email VARCHAR(50) NOT NULL,
                   customer_building INT NOT NULL,
                   customer_street VARCHAR(50) NOT NULL, 
                   customer_city VARCHAR(50) NOT NULL, 
                   customer_postcode VARCHAR(50) NOT NULL, 
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
                   order_id INT,
                   customer_id INT,
                   product_id INT,
                   product_qty INT NOT NULL,
                   PRIMARY KEY (order_id, customer_id, product_id),
                   FOREIGN KEY (customer_id) REFERENCES customer_basic_info(customer_id),
                   FOREIGN KEY (product_id) REFERENCES product(product_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_products_info;"))

# order_datetime
RSQLite::dbExecute(connection, "
                   CREATE TABLE order_datetime (
                   order_id INT,
                   customer_id INT,
                   order_date DATE NOT NULL, 
                   order_time TIME NOT NULL,
                   PRIMARY KEY (order_id, customer_id),
                   FOREIGN KEY (customer_id) REFERENCES customer_basic_info(customer_id)
                   );")

print(RSQLite::dbGetQuery(connection, "SELECT * FROM order_datetime;"))

# For Delivery
# delivery_tracking
RSQLite::dbExecute(connection, "
                   CREATE TABLE delivery_tracking (
                   tracking_number INT PRIMARY KEY, 
                   trans_id INT,
                   delivery_instructions VARCHAR(125) NOT NULL,
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



# Data Validation 

# List all files

all_files <- list.files("Dataset/")
all_files


prefix <- "hi_"
suffix <- "_dataset.csv"
all_files <- gsub("hi_","",all_files)
all_files <- gsub("_dataset.csv","",all_files)
all_files

  
  ## 1.  Check number of rows and columns
  
all_files <- list.files("Dataset/")

for (variable in all_files) {
  this_filepath <- paste0("Dataset/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  
  number_of_rows <- nrow(this_file_contents)
  number_of_columns <- ncol(this_file_contents)
  
  print(paste0("The file: ",variable,
               " has: ",
               format(number_of_rows,big.mark = ","),
               " rows and ",
               number_of_columns," columns"))
}


## 2. Check the data structure

all_files <- list.files("Dataset/")

for (variable in all_files) {
  this_filepath <- paste0("Dataset/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  data_structure<-str(this_file_contents)
  
  print(paste0(data_structure,
               "The file: ",variable,
               " has above data structure"))
}


## 3. Check for NULL values

all_files <- list.files("Dataset/")

for (variable in all_files) {
  this_filepath <- paste0("Dataset/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  null<-sum(is.na(this_file_contents))
  
  print(paste0("The file: ",variable,
               " has a total of ", null,
               " NULL values"))
}


## 4. Check that each primary key is unique in each table except for order

all_files <- list.files("Dataset/")

for (variable in all_files) {
  this_filepath <- paste0("Dataset/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  hi <- nrow(unique(this_file_contents[,1]))== nrow(this_file_contents)
  
  print(paste0("The file: ",variable,
               " has unique primary key ",
               hi," columns"))
}


## For order dataset

orderdate_dataset <- read.csv("Dataset/hi_order_datetime_dataset.csv")
orderproductsinfo_dataset <- read.csv("Dataset/hi_order_products_info_dataset.csv")
nrow(unique(orderdate_dataset[,1:2])) == nrow(orderdate_dataset)
nrow(unique(orderproductsinfo_dataset[,1:3])) == nrow(orderproductsinfo_dataset)
#sum(nrow(unique(orders[,1:2])))
#length((unique(orders$cutomer_id)))
#length((unique(orders$order_id)))
#The file: hi_order_dataset.csv has unique primary composite key TRUE columns


## 5. Validate phone number

# Supplier
supplier_dataset <- read.csv("Dataset/hi_supplier_dataset.csv")
length(grepl("\\+44\\s\\d{3}\\s\\d{3}\\s\\d{4}", supplier_dataset$supplier_phone)) == nrow(supplier_dataset)
# Customer
customer_basic_info_dataset <- read.csv("Dataset/hi_customer_basic_info_dataset.csv")
length(grepl("\\+44\\s\\d{3}\\s\\d{3}\\s\\d{4}", customer_basic_info_dataset$customer_phone)) == nrow(customer_basic_info_dataset)


## 6. Validate email address

# Supplier
length(grepl("@", supplier_dataset$supplier_email)) == nrow(supplier_dataset)
# Consumer
length(grepl("@", customer_basic_info_dataset$customer_email)) == nrow(customer_basic_info_dataset)



## 7. Referential Integrity Check

#Customer info - Promo_code
customer_basic_info_dataset <- read.csv("Dataset/hi_customer_basic_info_dataset.csv")
promotion_dataset <- read.csv("Dataset/hi_promotion_dataset.csv")

referential_integrity1 <- customer_basic_info_dataset  %>%
  anti_join(promotion_dataset, by = "promo_code")
if (nrow(referential_integrity1) == 0) {
  cat("customer - promo_code referential integrity check passed.\n")
} else {
  cat("customer - promo_code referential integrity check failed.\n")
  print(referential_integrity1)
}

#Customer info - Customer membership
customer_membership_dataset <- read.csv("Dataset/hi_customer_membership_dataset.csv")

referential_integrity2 <- customer_basic_info_dataset  %>%
  anti_join(customer_membership_dataset, by = "customer_id")
if (nrow(referential_integrity2) == 0) {
  cat("customer_info - customer membership referential integrity check passed.\n")
} else {
  cat("customer_info - customer membership referential integrity check failed.\n")
  print(referential_integrity2)
}

#Order-Customer-product
order_products_info_dataset <- read.csv("Dataset/hi_order_products_info_dataset.csv")
product_dataset <- read.csv("Dataset/hi_product_dataset.csv")

referential_integrity3 <- order_products_info_dataset  %>%
  anti_join(customer_basic_info_dataset, by = "customer_id")
if (nrow(referential_integrity3) == 0) {
  cat("order - customer_id referential integrity check passed.\n")
} else {
  cat("order - customer_info referential integrity check failed.\n")
  print(referential_integrity3)
}
referential_integrity4 <- order_products_info_dataset  %>%
  anti_join(product_dataset, by = "product_id")
if (nrow(referential_integrity4) == 0) {
  cat("order - product_id referential integrity check passed.\n")
} else {
  cat("order - product_id referential integrity check failed.\n")
  print(referential_integrity4)
}

#order_date_time - customer_info
order_datetime_dataset <- read.csv("Dataset/hi_order_datetime_dataset.csv")

referential_integrity5 <- order_datetime_dataset  %>%
  anti_join(customer_basic_info_dataset, by = "customer_id")
if (nrow(referential_integrity5) == 0) {
  cat("order_date_time - customer_info referential integrity check passed.\n")
} else {
  cat("order_date_time - customer_info referential integrity check failed.\n")
  print(referential_integrity5)
}

#delivery-transaction_id
delivery_tracking_dataset <- read.csv("Dataset/hi_delivery_tracking_dataset.csv")
transaction_dataset <- read.csv("Dataset/hi_transaction_dataset.csv")

referential_integrity6 <- delivery_tracking_dataset  %>%
  anti_join(transaction_dataset, by = "trans_id")
if (nrow(referential_integrity6) == 0) {
  cat("delivery- transaction_id referential integrity check passed.\n")
} else {
  cat("delivery- transaction_id referential integrity check failed.\n")
  print(referential_integrity6)
}

#estimated_deli - tracking_number
estimated_delivery_date_dataset <- read.csv("Dataset/hi_estimated_delivery_date_dataset.csv")

referential_integrity7 <- estimated_delivery_date_dataset  %>%
  anti_join(delivery_tracking_dataset, by = "tracking_number")
if (nrow(referential_integrity7) == 0) {
  cat("estimated_delivery - tracking_number referential integrity check passed.\n")
} else {
  cat("estimated_delivert - tracking_number referential integrity check failed.\n")
  print(referential_integrity7)
}

#actual_deli - tracking
actual_delivery_date_dataset <- read.csv("Dataset/hi_actual_delivery_date_dataset.csv")

referential_integrity8 <- actual_delivery_date_dataset  %>%
  anti_join(delivery_tracking_dataset, by = "tracking_number")
if (nrow(referential_integrity8) == 0) {
  cat("actual_delivery - tracking_number referential integrity check passed.\n")
} else {
  cat("actual_delivery - tracking_number referential integrity check failed.\n")
  print(referential_integrity8)
}


#product-supplier-category
supplier_dataset <- read.csv("Dataset/hi_supplier_dataset.csv")
product_category_dataset <- read.csv("Dataset/hi_product_category_dataset.csv")

referential_integrity9 <- product_dataset  %>%
  anti_join(supplier_dataset, by = "supplier_id")
if (nrow(referential_integrity9) == 0) {
  cat("product-supplier referential integrity check passed.\n")
} else {
  cat("product-supplier referential integrity check failed.\n")
  print(referential_integrity9)
}
referential_integrity10 <- product_dataset  %>%
  anti_join(product_category_dataset, by = "category_name")
if (nrow(referential_integrity10) == 0) {
  cat("product-category referential integrity check passed.\n")
} else {
  cat("product-category referential integrity check failed.\n")
  print(referential_integrity10)
}


#transaction - order
transaction_dataset <- read.csv("Dataset/hi_transaction_dataset.csv")

referential_integrity11 <- transaction_dataset  %>%
  anti_join(order_products_info_dataset, by = "order_id")
if (nrow(referential_integrity11) == 0) {
  cat("product-category referential integrity check passed.\n")
} else {
  cat("product-category referential integrity check failed.\n")
  print(referential_integrity11)
}



# Import csv files into SQL table
## Read datasets
### order
order_datetime_dataset <- read.csv("Dataset/hi_order_datetime_dataset.csv")
order_products_info_dataset <- read.csv("Dataset/hi_order_products_info_dataset.csv")
### delivery
actual_delivery_date_dataset <- read.csv("Dataset/hi_actual_delivery_date_dataset.csv")
delivery_tracking_dataset <- read.csv("Dataset/hi_delivery_tracking_dataset.csv")
estimated_delivery_date_dataset <- read.csv("Dataset/hi_estimated_delivery_date_dataset.csv")
### customer
customer_basic_info_dataset <- read.csv("Dataset/hi_customer_basic_info_dataset.csv")
customer_membership_dataset <- read.csv("Dataset/hi_customer_membership_dataset.csv")
product_dataset <- read.csv("Dataset/hi_product_dataset.csv")
product_category_dataset <- read.csv("Dataset/hi_product_category_dataset.csv")
promotion_dataset <- read.csv("Dataset/hi_promotion_dataset.csv")
supplier_dataset <- read.csv("Dataset/hi_supplier_dataset.csv")
transaction_dataset <- read.csv("Dataset/hi_transaction_dataset.csv")

## Import
dbWriteTable(connection, "product", product_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "product_category", product_category_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "promotion", promotion_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "supplier", supplier_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "transaction", transaction_dataset, append = TRUE, row.names = FALSE)
### Order
dbWriteTable(connection, "order_datetime", order_datetime_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "order_products_info", order_products_info_dataset, append = TRUE, row.names = FALSE)
### Delivery
dbWriteTable(connection, "actual_delivery_date", actual_delivery_date_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "delivery_tracking", delivery_tracking_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "estimated_delivery_date", estimated_delivery_date_dataset, append = TRUE, row.names = FALSE)
### Customer
dbWriteTable(connection, "customer_membership", customer_membership_dataset, append = TRUE, row.names = FALSE)
dbWriteTable(connection, "customer_basic_info", customer_basic_info_dataset, append = TRUE, row.names = FALSE)


# Check the tables using select
print(RSQLite::dbGetQuery(connection, "SELECT * FROM product LIMIT 5"))
print(RSQLite::dbGetQuery(connection, "SELECT * FROM product_category LIMIT 5"))
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
#RSQLite::dbDisconnect(connection)


# Data Validation
## Validate phone number
### Supplier
length(grepl("\\+44\\s\\d{3}\\s\\d{3}\\s\\d{4}", supplier$supplier_phone)) == nrow(supplier)
### Consumer
length(grepl("\\+44\\s\\d{3}\\s\\d{3}\\s\\d{4}", customer_basic_info$customer_phone)) == nrow(customer_basic_info)

## Validate emails
### Supplier
length(grepl("@", supplier$supplier_email)) == nrow(supplier)
### Consumer
length(grepl("@", customer_basic_info$customer_email)) == nrow(customer_basic_info)

