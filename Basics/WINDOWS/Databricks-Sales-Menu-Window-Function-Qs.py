



That's a lot of problems! I'll give you **10 intermediate-level PySpark problems** based on the inferred `sales_df` and `menu_df` schema. These problems focus on common analytical tasks using **Window Functions, Joins, and Aggregations**.

***

## 🧩 10 Intermediate PySpark Problems

### Data Setup Recap:

* **`sales_df`** (Transactions): `customer_id`, `product_id`, `order_date` (date/timestamp)
* **`menu_df`** (Products): `product_id`, `product_name`, `price`, `category` (inferred)

### Section A: Window Functions (Advanced Analytics)

These problems require a `WindowSpec` to calculate running or ranked metrics.

#### 1. Running Total Sales Per Customer
Find the **cumulative sum of money spent** by each customer, ordered by `order_date`. This reveals their total lifetime value over time.

#### 2. Days Since Last Visit
For each order, calculate the **number of days elapsed since the customer's previous order** (their time-lag between visits). Use a lead or lag function on the `order_date`.

#### 3. Monthly Spend Rank
In a given month, rank each customer based on their **total spent in that month**. The ranking should reset every month.

#### 4. Top 3 Most Expensive Items Purchased by Category
In **each food category**, find the **name and price of the 3 most expensive products** ever purchased by customers. Use a ranking function like `ROW_NUMBER()` or `DENSE_RANK()`.

***

### Section B: Aggregation & Joins (Complex Summaries)

These problems require complex grouping and sometimes multiple aggregation steps.

#### 5. Most Frequent First Purchase
Identify the single `product_name` that appears the **most often as a customer's very first purchase**.

#### 6. Average Order Value by Day of Week
Calculate the **average total value of a single order/bill** (`AVG(SUM(price))`) for each day of the week (Monday, Tuesday, etc.).

#### 7. Customer Cohort Analysis (Simplified)
For each unique month of a customer's **first purchase**, calculate the **total number of customers** who started that month.

#### 8. Products Never Sold
Identify all `product_name`s from the `menu_df` that **have not appeared** in the `sales_df` (i.e., have never been sold). Use an appropriate join type.

***

### Section C: Conditional & Rolling Aggregations

These problems require conditional logic (`when/otherwise`) within an aggregation or a simple rolling metric.

#### 9. Revenue Contribution by Category
Calculate the **total revenue** and the **percentage contribution** of each food category to the total company revenue.

#### 10. Customer Buying Streak
For each customer, find the **longest consecutive streak of days** where they placed at least one order. (This is highly advanced, but an intermediate solution could find the number of **consecutive weeks/months** with a purchase).
