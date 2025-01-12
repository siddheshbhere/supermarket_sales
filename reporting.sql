-- Top 3 product lines based on total sales in each branch

WITH CTE AS(
SELECT 
    b.branch,
    p.product_line,
    ROUND(SUM(s.total), 2) AS total_sales,
    RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(SUM(s.total), 2) DESC) AS r
FROM 
    sales s
JOIN 
    branch b ON s.branch_id = b.branch_id
JOIN 
    product p ON s.product_id = p.product_id
GROUP BY 
    b.branch, p.product_line
)
SELECT branch,
	   product_line,
	   total_sales 
FROM CTE 
WHERE r <= 3;

-- Top 3 product line based on average customer rating in each branch 

WITH CTE AS(
SELECT 
    b.branch,
    p.product_line,
    ROUND(AVG(s.rating), 2) AS avg_rating,
    RANK() OVER (PARTITION BY b.branch ORDER BY ROUND(AVG(s.rating), 2) DESC) AS r
FROM 
    sales s
JOIN 
    branch b ON s.branch_id = b.branch_id
JOIN 
    product p ON s.product_id = p.product_id
GROUP BY 
    b.branch, p.product_line
)
SELECT branch,
	   product_line,
	   avg_rating 
FROM CTE 
WHERE r <= 3;

-- Monthly Sales by Branch

SELECT 
    b.branch,
    strftime('%Y-%m', s.date) AS sales_month,
    ROUND(SUM(s.total), 2) AS total_sales
FROM 
    sales s
JOIN 
    branch b ON s.branch_id = b.branch_id
GROUP BY 
    b.branch, sales_month
ORDER BY 
    b.branch, sales_month;


-- Sales Distribution by Payment Method

SELECT 
    s.payment,
    COUNT(*) AS num_transactions,
    ROUND(SUM(s.total), 2) AS total_sales
FROM 
    sales s
GROUP BY 
    s.payment
ORDER BY 
    total_sales DESC;


