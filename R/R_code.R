library(readr)
library(RSQLite)

#Setup the connection
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"hi_import.db")

# Read datasets
delivery <- readr::read_csv("../data_upload/hi_delivery_dataset.csv")
orders <- readr::read_csv("../data_upload/hi_order_dataset.csv")
product <- readr::read_csv("../data_upload/hi_product_dataset.csv")
product_category <- readr::read_csv("../data_upload/hi_productcategory_dataset.csv")
promotion <- readr::read_csv("../data_upload/hi_promotion_dataset.csv")
supplier<- readr::read_csv("../data_upload/hi_supplier_dataset.csv")
transaction <- readr::read_csv("../data_upload/hi_transaction_dataset.csv")
membership_info <- readr::read_csv("../data_upload/hi_membership_dataset.csv")
estimated_delivery_date <- readr::read_csv("../data_upload/hi_estimateddeliverydate_dataset.csv")
actual_delivery_date <- readr::read_csv("../data_upload/hi_actualdeliverydate_dataset.csv")
order_date <- readr::read_csv("../data_upload/hi_orderdate_dataset.csv")
customer_basic_info <- readr::read_csv("../data_upload/hi_customerbasicinfo_dataset.csv")
order_products_info <- readr::read_csv("../data_upload/hi_orderproductsinfo_dataset.csv")
tracking_number <- readr::read_csv("../data_upload/hi_trackingnumber_dataset.csv")

# Write tables to the database
dbWriteTable(connection, 'delivery_table', delivery, append = TRUE)
dbWriteTable(connection, 'orders_table', order, append = TRUE)
dbWriteTable(connection, 'product_table', product, append = TRUE)
dbWriteTable(connection, 'product_category_table', product_category, append = TRUE)
dbWriteTable(connection, 'promotion_table', promotion, append = TRUE)
dbWriteTable(connection, 'supplier_table', supplier, append = TRUE)
dbWriteTable(connection, 'transactions_table', transaction, append = TRUE)
dbWriteTable(connection, 'membership_table', membership_info, append = TRUE)
dbWriteTable(connection, 'estimated_delivery_date_table', estimated_delivery_date, append = TRUE)
dbWriteTable(connection, 'actual_delivery_date_table', actual_delivery_date, append = TRUE)
dbWriteTable(connection, 'order_date_table', order_date, append = TRUE)
dbWriteTable(connection, 'customer_basic_info_table', customer_basic_info, append = TRUE)
dbWriteTable(connection, 'order_products_info_table', order_products_info, append = TRUE)
dbWriteTable(connection, 'delivery_tracking_table', tracking_number, append = TRUE)


