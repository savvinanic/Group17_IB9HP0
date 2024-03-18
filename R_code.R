library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Load Files in an sqlite database 
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"hi_import.db")

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


#Plot graphs

#Upload required packages 
library(ggplot2)
library(readr)
library(RSQLite)
library(dplyr)
library(DBI)
library(ggplot2)
library(tidyverse)
library(emmeans)
library(gridExtra)
library(knitr)


# 1. Product Category vs Count
product_procat_join <- dbGetQuery(connection, "
  SELECT p.*, pc.parent_category_id
  FROM product AS p
  INNER JOIN product_category AS pc ON p.category_name = pc.category_name
")

product_count <- product_procat_join %>%
  group_by(parent_category_id) %>%
  summarise(count = n())

g1 <- ggplot(product_count, aes(x = factor(parent_category_id), y = count, fill = factor(parent_category_id))) +
  geom_bar(stat = "identity", position = "dodge", color = "black", alpha = 0.7) +
  labs(title = "Product Count by Parent Category",
       x = "Parent Category",
       y = "Count") +
  scale_fill_brewer(name = "Parent Category", palette = "Set3") +
  theme_minimal() +  # Apply minimal theme
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("figures/Product Category vs Count.png", plot = g1, width = 10, height = 6)

#  Sales by Category
order_product_join <- dbGetQuery(connection, "
SELECT op.order_id, op.customer_id, op.product_id, p.parent_category_id, op.product_qty, p.product_price, od.order_date
FROM `order_products_info` AS op
INNER JOIN order_datetime AS od ON op.order_id = od.order_id
INNER JOIN product_procat_join_dataset AS p ON op.product_id = p.product_id
")

category_sales <- order_product_join %>%
  group_by(parent_category_id) %>%
  summarise(total_sales = sum(product_qty * product_price))

my_colors <- c("#1f78b4", "#33a02c", "#e31a1c", "#ff7f00", "#6a3d9a", "#b15928", "#a6cee3", "#b2df8a", "#fb9a99", "#fdbf6f")

g2 <- ggplot(category_sales, aes(x = factor(parent_category_id), y = total_sales, fill = factor(parent_category_id))) +
  geom_bar(stat = "identity", color = "black") +
  scale_fill_manual(values = my_colors) +
  labs(x = "Categories", y = "Total Sale (in £)", title = "Category-wise Sales") +
  theme_minimal() +
  theme(legend.position = "none")  

ggsave("figures/Sales_by_Category.png", plot = g2, width = 10, height = 6)


# 3. Product Category vs Average Order Quantity per Order
orderqty_category_join <- dbGetQuery(connection, "
SELECT pc.parent_category_id, AVG(o.product_qty) AS avg_order_quantity
FROM `order_products_info` AS o
INNER JOIN product AS p ON o.product_id = p.product_id
INNER JOIN product_category AS pc ON p.category_name = pc.category_name
GROUP BY pc.parent_category_id
")

g3 <- ggplot(orderqty_category_join, aes(x = factor(parent_category_id), 
                                   y = avg_order_quantity, 
                                   fill = parent_category_id)) +
  geom_bar(stat = "identity", 
           position = "dodge", 
           color = "black", 
           alpha = 0.7) +
  labs(title = "Avg. Order Quantity by Product Category per Order",
       x = "Product Category",
       y = "Avg. Order Quantity / Order") +
  scale_fill_discrete(name = "Product Category") +
  theme_minimal() +  
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

ggsave("figures/Avg order quantity.png", plot = g3, width = 10, height = 6)

# 4. Product Category vs Average Order Quantity per Month
monthly_orderqty_category_join <- dbGetQuery(connection, "
SELECT pc.parent_category_id, o.order_date, o.product_qty
FROM `order_products_datetime_dataset` AS o
INNER JOIN product AS p ON o.product_id = p.product_id
INNER JOIN product_category AS pc ON p.category_name = pc.category_name
")

# Convert order_date to date format
monthly_orderqty_category_join <- monthly_orderqty_category_join %>%
  mutate(order_date = as.Date(order_date))

# Extract month from order_date
monthly_orderqty_category_join <- monthly_orderqty_category_join %>%
  mutate(month = format(order_date, "%m-%Y"))

# Calculate average order quantity for each product and each month
avg_order_quantity <- monthly_orderqty_category_join %>%
  group_by(parent_category_id, month) %>%
  summarise(avg_qty = mean(product_qty))

# Per product category
g4 <- ggplot(avg_order_quantity, aes(x = month, y = avg_qty, fill = month)) +
  geom_bar(stat = "identity", position = "dodge", color = "black", alpha = 0.7) +
  labs(title = "Average Order Quantity for Each Product Category",
       x = "Month",
       y = "Average Order Quantity",
       fill = "Month") +
  scale_fill_brewer(palette = "Set3", name = "Month") +
  facet_wrap(~parent_category_id, scales = "free_y") +
  theme(axis.text.x = element_blank(),  # Remove x-axis labels
        axis.ticks.x = element_blank()) # Remove x-axis ticks

ggsave("figures/Product Category&Avg quantity per month.png", plot = g4, width = 10, height = 6)

# 5. Number of Orders Placed Each Hour of the Day

order_copyforanalyses <- order_datetime

# Convert 'order_time' column to POSIXct format
order_copyforanalyses$order_time <- as.POSIXct(order_copyforanalyses$order_time, format = "%H:%M:%S")

# Extract hour from 'order_time' and create a new column 'order_hour'
order_copyforanalyses$order_hour <- format(order_copyforanalyses$order_time, format = "%H")

order_copyforanalyses$order_hour <- as.numeric(order_copyforanalyses$order_hour)

g5 <- ggplot(order_copyforanalyses, aes(x = order_hour)) +
  geom_histogram(binwidth = 1, fill = "#4682B4", color = "black") +  # Adjusted fill color
  scale_x_continuous(breaks = seq(0, 24, by = 1)) +
  labs(x = "Order Hour", y = "Number of Orders",
       title = "Distribution of Orders by Hour of the Day") +
  theme_minimal() +
  theme(plot.title = element_text(size = 14, face = "bold"),
        axis.title = element_text(size = 12),
        axis.text = element_text(size = 10))

ggsave("figures/Orders by hour.png", plot = g5, width = 10, height = 6)



# 6. Percentage of Customers Having a Membership
membership <- customer_membership %>%
  group_by(customer_membership) %>%
  summarise(count = n()) %>%
  mutate(total = sum(count)) %>%
  mutate(membership_percentage = (count / total) * 100)

labels <- paste(membership$customer_membership, 
                "-", 
                round(membership$membership_percentage, 2), "%")

pie_chart <- pie(membership$membership_percentage, 
    labels = labels, 
    main = "figures/Membership Percentage", 
    clockwise = TRUE)

ggsave("figures/Membership_Pie_Chart.png", plot = pie_chart, width = 8, height = 6)

# 7. Sales by Membership Status
customer_order_join <- dbGetQuery(connection, "
SELECT c.customer_id, c.customer_membership, o.order_id, o.product_id, o.product_qty, p.product_price
FROM `customer_membership` AS c
INNER JOIN order_products_info AS o ON c.customer_id = o.customer_id
INNER JOIN product AS p ON o.product_id = p.product_id
")

customer_order_join$total_product_price <- customer_order_join$product_qty * customer_order_join$product_price

customer_order_join.bycustomer <- customer_order_join %>% 
  group_by(customer_id) %>% 
  summarise(membership_status = first(customer_membership), Total_sale = sum(total_product_price))

mean_totals <- customer_order_join.bycustomer %>%
  group_by(membership_status) %>%
  summarise(Total_sale = sum(Total_sale))

g7 <- ggplot(mean_totals, aes(x = factor(membership_status), y = Total_sale, fill = factor(membership_status), label = Total_sale)) +
  geom_bar(stat = "identity", color = "black", width = 0.6) +  
  labs(fill = "Membership Status", x = "Membership Status", y = "Total Sale", title = "Total Sales by Membership Status") +
  theme_minimal() + 
  scale_fill_manual(values = my_colors) + 
  geom_text(aes(label = paste0("£", scales::comma_format()(Total_sale))), position = position_stack(vjust = 0.5), vjust = -0.5, size = 4) +  
  scale_y_continuous(labels = scales::dollar_format(prefix = "£", suffix = ""))

ggsave("figures/Sales_by_membership status.png", plot = g7, width = 10, height = 6)


# 8. Order Delay - Membership
Orderdelay_membership <- dbGetQuery(connection, "
SELECT dd.tracking_number, dd.delay, dt.trans_id, t.order_id, op.customer_id, c.customer_membership
FROM `delivered_deliveries_dataset` AS dd
INNER JOIN `delivery_tracking` AS dt ON dd.tracking_number = dt.tracking_number
INNER JOIN `transaction` AS t ON dt.trans_id = t.trans_id
INNER JOIN `order_products_info` AS op ON t.order_id = op.order_id
INNER JOIN `customer_membership` AS c ON op.customer_id = c.customer_id
")

g8 <- ggplot(Orderdelay_membership) + 
  geom_boxplot(aes(x = factor(customer_membership), y = delay)) + 
  labs(x = "Membership Status", y = "Delays in Delivery", title = "Delivery Delays by Membership Status") + 
  theme(plot.title = element_text(size = 12, face = "bold", hjust = 0.5))

ggsave("figures/Order_delay.png", plot = g8, width = 10, height = 6)

# 9.Monthly Revenue by City
Revenue <- dbGetQuery(connection, "
SELECT o.customer_id, c.customer_city, o.order_id, d.order_date, ROUND(SUM(p.product_price * o.product_qty) * (1 - CAST(prm.percentage_discount AS REAL) / 100), 2) AS revenue
FROM 'order_products_info' o, 'product' p, 'promotion' prm, 'customer_basic_info' c, 'customer_membership' m, 'order_datetime' d
WHERE o.customer_id = m.customer_id AND o.product_id = p.product_id AND c.promo_code = prm.promo_code AND m.customer_id = c.customer_id AND o.order_id = d.order_id
GROUP BY o.order_id;
")

City_vs_Revenue <- Revenue %>%
  mutate(
    order_date = as.Date(order_date),
    month = as.Date(format(order_date, "%Y-%m-01")) # Ensures correct chronological ordering
  ) %>%
  group_by(customer_city, month) %>%
  summarise(total_revenue_month = sum(revenue), .groups = 'drop') %>%
  arrange(customer_city, month) 

g9 <- ggplot(City_vs_Revenue, aes(x = month, y = total_revenue_month, group = customer_city, color = customer_city)) +
  geom_line() +
  labs(title = "Monthly Revenue by City",
       x = "Month",
       y = "Total Revenue") +
  scale_x_date(date_labels = "%m-%Y", date_breaks = "1 month", name = "Month") +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

ggsave("figures/Monthly Revenue by City.png", plot = g9, width = 10, height = 6)












# 11. Top 5 Products by Sales Volume and Total Sales Profit After Discount

RSQLite::dbExecute(connection, "
SELECT p.product_name, 
       SUM(o.product_qty) AS sales_volume, 
       ROUND(SUM(p.product_price * o.product_qty) * (1 - CAST(prm.percentage_discount AS REAL) / 100), 2) AS total_sales_profit
FROM order_products_info AS o
INNER JOIN product AS p ON p.product_id = p.product_id
INNER JOIN promotion AS prm ON o.promo_code = prm.promo_code
INNER JOIN customer_basic_info AS c ON o.customer_id = c.customer_id
INNER JOIN customer_membership AS m ON c.customer_id = m.customer_id
GROUP BY p.product_name
ORDER BY total_sales_profit DESC LIMIT 5;
")


# 12. 

RSQLite::dbExecute(connection,"
SELECT o.customer_id, 
       o.order_id, 
       prm.percentage_discount, 
       ROUND(SUM(p.product_price * o.product_qty) * (1 - CAST(prm.percentage_discount AS REAL) / 100) + m.delivery_fee, 2) AS trans_amount
FROM order_products_info AS o
JOIN product AS p ON o.product_id = p.product_id
JOIN customer_basic_info AS c ON o.customer_id = c.customer_id
JOIN promotion AS prm ON c.promo_code = prm.promo_code
JOIN customer_membership AS m ON c.customer_id = m.customer_id
GROUP BY o.order_id
ORDER BY trans_amount DESC 
LIMIT 10
")


