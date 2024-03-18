#Plot graphs
#Upload required packages 
library(ggplot2)
library(readr)
library(RSQLite)
library(dplyr)
library(DBI)
library(ggplot2)
library(gridExtra)
library(knitr)

# Load Files in an sqlite database 
connection <- RSQLite::dbConnect(RSQLite::SQLite(),"hi_import.db")


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

# Retrieve data from the "order_datetime" table
order_copyforanalyses <- DBI::dbGetQuery(connection, "SELECT * FROM order_datetime")


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
membership <- DBI::dbGetQuery(connection, "SELECT * FROM customer_membership") %>%
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




