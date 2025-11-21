-- ========================================
-- TABLEAU CUSTOM SQL QUERIES
-- Multi-table joins, CTEs, Window Functions
-- ========================================

-- ========================================
-- 1. COMPREHENSIVE SALES ANALYSIS WITH JOINS
-- ========================================
SELECT 
    s.sale_id,
    s.sale_date,
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    c.country,
    p.product_id,
    p.product_name,
    p.category,
    s.quantity,
    s.unit_price,
    s.total_amount,
    p.cost,
    (s.unit_price - p.cost) * s.quantity AS profit,
    ROUND(((s.unit_price - p.cost) / s.unit_price) * 100, 2) AS margin_percent,
    s.payment_method,
    s.status
FROM sales s
INNER JOIN customers c ON s.customer_id = c.customer_id
INNER JOIN products p ON s.product_id = p.product_id
WHERE s.status = 'Completed';

-- ========================================
-- 2. CTE: MONTHLY SALES TRENDS WITH GROWTH
-- ========================================
WITH monthly_sales AS (
    SELECT 
        DATE_FORMAT(sale_date, '%Y-%m') AS sale_month,
        SUM(total_amount) AS monthly_revenue,
        COUNT(sale_id) AS transaction_count,
        AVG(total_amount) AS avg_order_value
    FROM sales
    WHERE status = 'Completed'
    GROUP BY DATE_FORMAT(sale_date, '%Y-%m')
),
sales_with_lag AS (
    SELECT 
        sale_month,
        monthly_revenue,
        transaction_count,
        avg_order_value,
        LAG(monthly_revenue, 1) OVER (ORDER BY sale_month) AS prev_month_revenue
    FROM monthly_sales
)
SELECT 
    sale_month,
    monthly_revenue,
    transaction_count,
    avg_order_value,
    prev_month_revenue,
    ROUND(((monthly_revenue - prev_month_revenue) / prev_month_revenue) * 100, 2) AS revenue_growth_pct
FROM sales_with_lag
ORDER BY sale_month;

-- ========================================
-- 3. WINDOW FUNCTION: CUSTOMER RANKING
-- ========================================
SELECT 
    c.customer_id,
    c.customer_name,
    c.city,
    c.state,
    COUNT(s.sale_id) AS total_orders,
    SUM(s.total_amount) AS total_spent,
    AVG(s.total_amount) AS avg_order_value,
    RANK() OVER (ORDER BY SUM(s.total_amount) DESC) AS customer_rank,
    ROW_NUMBER() OVER (PARTITION BY c.state ORDER BY SUM(s.total_amount) DESC) AS state_rank
FROM customers c
LEFT JOIN sales s ON c.customer_id = s.customer_id
WHERE s.status = 'Completed'
GROUP BY c.customer_id, c.customer_name, c.city, c.state
ORDER BY total_spent DESC;

-- ========================================
-- 4. CTE: PRODUCT PERFORMANCE ANALYSIS
-- ========================================
WITH product_metrics AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        p.price,
        p.cost,
        COUNT(s.sale_id) AS times_sold,
        SUM(s.quantity) AS total_units_sold,
        SUM(s.total_amount) AS total_revenue,
        SUM((s.unit_price - p.cost) * s.quantity) AS total_profit
    FROM products p
    LEFT JOIN sales s ON p.product_id = s.product_id AND s.status = 'Completed'
    GROUP BY p.product_id, p.product_name, p.category, p.price, p.cost
)
SELECT 
    product_id,
    product_name,
    category,
    times_sold,
    total_units_sold,
    total_revenue,
    total_profit,
    ROUND(total_profit / total_revenue * 100, 2) AS profit_margin,
    PERCENT_RANK() OVER (ORDER BY total_revenue) AS revenue_percentile
FROM product_metrics
WHERE total_revenue > 0
ORDER BY total_revenue DESC;

-- ========================================
-- 5. WINDOW FUNCTION: RUNNING TOTALS
-- ========================================
SELECT 
    sale_date,
    sale_id,
    customer_id,
    total_amount,
    SUM(total_amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    AVG(total_amount) OVER (ORDER BY sale_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7day
FROM sales
WHERE status = 'Completed'
ORDER BY sale_date;

-- ========================================
-- 6. CTE: CUSTOMER COHORT ANALYSIS
-- ========================================
WITH first_purchase AS (
    SELECT 
        customer_id,
        MIN(DATE_FORMAT(sale_date, '%Y-%m')) AS cohort_month
    FROM sales
    WHERE status = 'Completed'
    GROUP BY customer_id
),
customer_orders AS (
    SELECT 
        s.customer_id,
        fp.cohort_month,
        DATE_FORMAT(s.sale_date, '%Y-%m') AS order_month,
        s.total_amount
    FROM sales s
    INNER JOIN first_purchase fp ON s.customer_id = fp.customer_id
    WHERE s.status = 'Completed'
)
SELECT 
    cohort_month,
    order_month,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(total_amount) AS cohort_revenue
FROM customer_orders
GROUP BY cohort_month, order_month
ORDER BY cohort_month, order_month;

-- ========================================
-- 7. SUMMARY TABLE: EXECUTIVE DASHBOARD
-- ========================================
SELECT 
    DATE_FORMAT(s.sale_date, '%Y-%m') AS month,
    COUNT(DISTINCT s.customer_id) AS unique_customers,
    COUNT(s.sale_id) AS total_orders,
    SUM(s.total_amount) AS total_revenue,
    AVG(s.total_amount) AS avg_order_value,
    SUM((s.unit_price - p.cost) * s.quantity) AS total_profit,
    ROUND(SUM((s.unit_price - p.cost) * s.quantity) / SUM(s.total_amount) * 100, 2) AS profit_margin_pct
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
WHERE s.status = 'Completed'
GROUP BY DATE_FORMAT(s.sale_date, '%Y-%m')
ORDER BY month DESC;
