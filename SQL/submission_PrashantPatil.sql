/*

-----------------------------------------------------------------------------------------------------------------------------------
													SQL and Databases project by Prashant Patil
                                                    AIML Online October 2023-A Batch
                                                    04-May-2024
-----------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------------------------
                                                         Queries
                                               
-----------------------------------------------------------------------------------------------------------------------------------*/
/*---------------------------------Declare database-----------------------------------*/
  USE newwheels;
/*------------------------------------------------------------------------------------*/
  
  
/*-- QUESTIONS RELATED TO CUSTOMERS
     
     
     [Q1] What is the distribution of customers across states?
     Hint: For each state, count the number of customers.*/
     
     -- ****Solution*****  
SELECT 
    state, COUNT(customer_id) AS customer_count
FROM
    newwheels.customer_t
GROUP BY state
ORDER BY customer_count DESC;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q2] What is the average rating in each quarter?
-- Very Bad is 1, Bad is 2, Okay is 3, Good is 4, Very Good is 5.

Hint: Use a common table expression and in that CTE, assign numbers to the different customer ratings. 
      Now average the feedback for each quarter. */

-- ****Solution*****
-- Lets create common table expression that will give customer feedback in the form of numeric values. Then lets average it out group by quarters
WITH RatingNumbers AS (
    SELECT
        quarter_number,
        CASE
            WHEN customer_feedback = 'Very Bad' THEN 1
            WHEN customer_feedback = 'Bad' THEN 2
            WHEN customer_feedback = 'Okay' THEN 3
            WHEN customer_feedback = 'Good' THEN 4
            WHEN customer_feedback = 'Very Good' THEN 5
            ELSE NULL  -- Handling any other values, if present
        END AS rating_number
    FROM
        order_t
    WHERE
        customer_feedback IN ('Very Bad', 'Bad', 'Okay', 'Good', 'Very Good')
)

SELECT
    quarter_number,
    AVG(rating_number) AS average_rating
FROM
    RatingNumbers
GROUP BY
    quarter_number
ORDER BY
    quarter_number;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q3] Are customers getting more dissatisfied over time?

Hint: Need the percentage of different types of customer feedback in each quarter. Use a common table expression and
	  determine the number of customer feedback in each category as well as the total number of customer feedback in each quarter.
	  Now use that common table expression to find out the percentage of different types of customer feedback in each quarter.
      Eg: (total number of very good feedback/total customer feedback)* 100 gives you the percentage of very good feedback.
      */
-- ****Solution*****
-- Lets use CTE for getting numeric rating(feedback) so that % for each quarter can be calculated 
WITH FeedbackCounts AS (
    SELECT quarter_number,
           COUNT(*) AS total_feedback,
           SUM(CASE WHEN customer_feedback = 'Very Bad' THEN 1 ELSE 0 END) AS very_bad_count,
           SUM(CASE WHEN customer_feedback = 'Bad' THEN 1 ELSE 0 END) AS bad_count,
           SUM(CASE WHEN customer_feedback = 'Okay' THEN 1 ELSE 0 END) AS okay_count,
           SUM(CASE WHEN customer_feedback = 'Good' THEN 1 ELSE 0 END) AS good_count,
           SUM(CASE WHEN customer_feedback = 'Very Good' THEN 1 ELSE 0 END) AS very_good_count
    FROM order_t
    GROUP BY quarter_number
)
SELECT
    quarter_number,
    total_feedback,
    very_bad_count,
    bad_count,
    okay_count,
    good_count,
    very_good_count,
    ROUND((very_bad_count / total_feedback) * 100, 2) AS very_bad_percentage,
    ROUND((bad_count / total_feedback) * 100, 2) AS bad_percentage,
    ROUND((okay_count / total_feedback) * 100, 2) AS okay_percentage,
    ROUND((good_count / total_feedback) * 100, 2) AS good_percentage,
    ROUND((very_good_count / total_feedback) * 100, 2) AS very_good_percentage
FROM FeedbackCounts
ORDER BY quarter_number;

-- Results shows that there is gradual decrease in Good feedback / rating as with every quarter. Also there is gradual increase in bad feedback/ratings with every quarter. Clearly indicates disstatisfaction of customer over time. 

-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q4] Which are the top 5 vehicle makers preferred by the customer.

Hint: For each vehicle make what is the count of the customers.*/

-- ****Solution*****
-- Lets count customers for various vehicle makers so that top 5 vehicle makers can be detarmined
SELECT 
    p.vehicle_maker, COUNT(o.customer_id) AS customer_count
FROM
    order_t o
        JOIN
    customer_t c ON o.customer_id = c.customer_id
        JOIN
    product_t p ON o.product_id = p.product_id
GROUP BY p.vehicle_maker
ORDER BY customer_count DESC
LIMIT 5;

-- ---------------------------------------------------------------------------------------------------------------------------------

/*[Q5] What is the most preferred vehicle make in each state?

Hint: Use the window function RANK() to rank based on the count of customers for each state and vehicle maker. 
After ranking, take the vehicle maker whose rank is 1.*/

-- ****Solution*****
-- Lets use CTE for finding Vehicle makers wise customer count using RANK function. Then we will use state wise customer count to have correct order of data for displaying on chart.

WITH StateVehicleCounts AS (
    SELECT
        state,
        vehicle_maker,
        COUNT(DISTINCT o.customer_id) AS customer_count,
        RANK() OVER (PARTITION BY state ORDER BY COUNT(DISTINCT o.customer_id) DESC) AS rank1
    FROM
        order_t o
    JOIN
        customer_t c ON o.customer_id = c.customer_id
    JOIN
        product_t p ON o.product_id = p.product_id
    GROUP BY state, vehicle_maker
),
StateCustomerCounts AS (
    SELECT
        state,
        count(o.customer_id) as state_customer_count,
        RANK() OVER (ORDER BY COUNT(DISTINCT o.customer_id) DESC) AS state_rank
    FROM
        order_t o
    JOIN
        customer_t c ON o.customer_id = c.customer_id
    GROUP BY state
)
SELECT 
    svc.state AS state,
    CONCAT(GROUP_CONCAT(CONCAT(svc.vehicle_maker)
                SEPARATOR ', ')) AS vehicle_maker,
    ROUND(AVG(svc.customer_count)) AS Top_makers_customer_count,
    ROUND(AVG(scc.state_customer_count)) AS Overall_state_customer_count,
    scc.state_rank AS state_rank
FROM
    StateVehicleCounts svc
        JOIN
    StateCustomerCounts scc ON svc.state = scc.state
WHERE
    svc.rank1 = 1
GROUP BY svc.state
ORDER BY AVG(scc.state_rank) ASC;


-- ---------------------------------------------------------------------------------------------------------------------------------

/*QUESTIONS RELATED TO REVENUE and ORDERS 

-- [Q6] What is the trend of number of orders by quarters?

Hint: Count the number of orders for each quarter.*/

-- ****Solution*****
-- Lets count orders for eaach quarters

SELECT 
    quarter_number, COUNT(order_id) AS num_orders
FROM
    order_t
GROUP BY quarter_number
ORDER BY quarter_number;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q7] What is the quarter over quarter % change in revenue? 

Hint: Quarter over Quarter percentage change in revenue means what is the change in revenue from the subsequent quarter to the previous quarter in percentage.
      To calculate you need to use the common table expression to find out the sum of revenue for each quarter.
      Then use that CTE along with the LAG function to calculate the QoQ percentage change in revenue.
*/

-- ****Solution*****
-- Lets use CTE to find quarterly revenue. Lets use LAG function to determine previous quarters revenue so that change % can be calculated.
WITH QuarterlyRevenue AS (
    SELECT
        quarter_number,
        SUM(quantity * vehicle_price) AS revenue
    FROM
        order_t
    GROUP BY quarter_number
)

SELECT
    quarter_number,
    revenue,
    LAG(revenue) OVER (ORDER BY quarter_number) AS prev_quarter_revenue,
    CASE
        WHEN LAG(revenue) OVER (ORDER BY quarter_number) = 0 THEN NULL
        ELSE ((revenue - LAG(revenue) OVER (ORDER BY quarter_number)) / LAG(revenue) OVER (ORDER BY quarter_number)) * 100
    END AS qoq_percentage_change
FROM
    QuarterlyRevenue
ORDER BY quarter_number;      
      

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q8] What is the trend of revenue and orders by quarters?

Hint: Find out the sum of revenue and count the number of orders for each quarter.*/

-- ****Solution*****
-- We will use CTE to find revenue and order count for each quarter. Also we will find change % in both fields using lag function.

WITH QuarterlyData AS (
    SELECT
        quarter_number,
        SUM(quantity * vehicle_price) AS revenue,
        COUNT(order_id) AS num_orders
    FROM
        order_t
    GROUP BY
        quarter_number
)

SELECT
    QD.quarter_number,
    QD.revenue,    
    QD.num_orders,    
    CASE
        WHEN LAG(QD.revenue) OVER (ORDER BY QD.quarter_number) = 0 THEN NULL
        ELSE ((QD.revenue - LAG(QD.revenue) OVER (ORDER BY QD.quarter_number)) / LAG(QD.revenue) OVER (ORDER BY QD.quarter_number)) * 100
    END AS revenue_change_percentage,
    CASE
        WHEN LAG(QD.num_orders) OVER (ORDER BY QD.quarter_number) = 0 THEN NULL
        ELSE ((QD.num_orders - LAG(QD.num_orders) OVER (ORDER BY QD.quarter_number)) / LAG(QD.num_orders) OVER (ORDER BY QD.quarter_number)) * 100
    END AS order_change_percentage
FROM
    QuarterlyData QD
ORDER BY
    QD.quarter_number;


-- ---------------------------------------------------------------------------------------------------------------------------------

/* QUESTIONS RELATED TO SHIPPING 
    [Q9] What is the average discount offered for different types of credit cards?

Hint: Find out the average of discount for each credit card type.*/

-- ****Solution*****
-- Lets find average discount offered by each credit card type
SELECT 
    ct.credit_card_type, AVG(ot.discount) AS average_discount
FROM
    customer_t ct
        JOIN
    order_t ot ON ct.customer_id = ot.customer_id
GROUP BY ct.credit_card_type
ORDER BY average_discount DESC;

-- ---------------------------------------------------------------------------------------------------------------------------------

/* [Q10] What is the average time taken to ship the placed orders for each quarters?
	Hint: Use the dateiff function to find the difference between the ship date and the order date.
*/

-- ****Solution*****
-- Lets use DateDiff function to find average ship time in days to ship vehicles for each quarter
SELECT 
    quarter_number,
    AVG(DATEDIFF(ship_date, order_date)) AS average_ship_time
FROM
    order_t
GROUP BY quarter_number
ORDER BY quarter_number;

-- --------------------------------------------------------------------------------------------------------------------------------
												-- Additional analysis : 
-- --------------------------------------------------------------------------------------------------------------------------------
-- Lets get the data to find correlation in python */
-- ****Solution*****
-- Note : Here we are finding raw data. Few categorical fields will be encoded in python so that correlation of all numeric values can be found.
WITH RatingNumbers AS (
    SELECT
        o.product_id,
        CASE
            WHEN o.customer_feedback = 'Very Bad' THEN 1
            WHEN o.customer_feedback = 'Bad' THEN 2
            WHEN o.customer_feedback = 'Okay' THEN 3
            WHEN o.customer_feedback = 'Good' THEN 4
            WHEN o.customer_feedback = 'Very Good' THEN 5
            ELSE NULL  -- Handling any other values, if present
        END AS rating_number
    FROM
        order_t o
    JOIN
        product_t p ON o.product_id = p.product_id
	
    WHERE
        o.customer_feedback IN ('Very Bad', 'Bad', 'Okay', 'Good', 'Very Good')
)

 SELECT
        p.vehicle_maker, p.vehicle_color, p.vehicle_model, p.vehicle_model_year, c.gender, c.job_title, c.state, c.city, c.credit_card_type,
        o.quarter_number as QuarterNo , o.discount, o.quantity,o.ship_mode,o.shipping,o.vehicle_price as Price, o.quantity * o.vehicle_price AS Revenue, o.product_id as Product,
        rn.rating_number Customers_Feedback,
        (DATEDIFF(o.ship_date, o.order_date)) AS ship_time
    FROM
        order_t o
    JOIN
        product_t p ON o.product_id = p.product_id 
	JOIN
		customer_t c ON c.customer_id = o.customer_id
    LEFT JOIN
        RatingNumbers rn ON o.product_id = rn.product_id;

-- ---------------------------------------------------------------------------------------------------------------------------------

-- -------------------------------------------Business overview----------------------------------------------------------
-- Lets determine values for various metrics items like total revenue, orders, top performing state(based on revenue), 
-- average ship time, top performing maker (based on revenue), customers feedback, categorywie feedback for each quarter.
-- alter This will be helpful for finding business overview
WITH RatingNumbers AS (
    SELECT
        order_id,
        quarter_number,
        CASE
            WHEN customer_feedback = 'Very Bad' THEN 1
            WHEN customer_feedback = 'Bad' THEN 2
            WHEN customer_feedback = 'Okay' THEN 3
            WHEN customer_feedback = 'Good' THEN 4
            WHEN customer_feedback = 'Very Good' THEN 5
            ELSE NULL  -- Handling any other values, if present
        END AS rating_number
    FROM
        order_t
    WHERE
        customer_feedback IN ('Very Bad', 'Bad', 'Okay', 'Good', 'Very Good')
),
CategoryFeedback AS (
    SELECT
        quarter_number,
        ROUND((SUM(CASE WHEN rating_number = 1 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS very_bad_percentage,
        ROUND((SUM(CASE WHEN rating_number = 2 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS bad_percentage,
        ROUND((SUM(CASE WHEN rating_number = 3 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS okay_percentage,
        ROUND((SUM(CASE WHEN rating_number = 4 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS good_percentage,
        ROUND((SUM(CASE WHEN rating_number = 5 THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS very_good_percentage
    FROM
        RatingNumbers
    GROUP BY
        quarter_number
),
OrderMetrics AS (
    SELECT
        o.quarter_number,
        COUNT(o.order_id) AS order_count,
        SUM(o.quantity * p.vehicle_price) AS revenue,
        AVG(DATEDIFF(ship_date, order_date)) AS avg_ship_time,
        AVG(r.rating_number) AS avg_rating,
        AVG(o.discount) AS Avg_discount
    FROM
        order_t o
    JOIN
        RatingNumbers r ON o.order_id = r.order_id
    JOIN
        product_t p ON o.product_id = p.product_id
    GROUP BY
        o.quarter_number
),
TopVehicleMaker AS (
    SELECT
        quarter_number,
        p.vehicle_maker,
        SUM(o.quantity * p.vehicle_price) AS revenue
    FROM
        order_t o
    JOIN
        product_t p ON o.product_id = p.product_id
    GROUP BY
        quarter_number, p.vehicle_maker
    ORDER BY
        quarter_number, revenue DESC
    LIMIT 1
),
TopState AS (
    SELECT
        quarter_number,
        c.state,
        SUM(o.quantity * p.vehicle_price) AS revenue
    FROM
        order_t o
    JOIN
        customer_t c ON o.customer_id = c.customer_id
    JOIN
        product_t p ON o.product_id = p.product_id
    GROUP BY
        quarter_number, c.state
    ORDER BY
        quarter_number, revenue DESC
    LIMIT 1
)

SELECT
    o.quarter_number,
    o.order_count,
    o.revenue,
    c.very_bad_percentage,
    c.bad_percentage,
    c.okay_percentage,
    c.good_percentage,
    c.very_good_percentage,
    o.avg_ship_time,
    o.avg_rating,
    tvm.vehicle_maker AS top_vehicle_maker,
    ts.state AS top_state,
    o.Avg_discount
FROM
    OrderMetrics o
JOIN
    CategoryFeedback c ON o.quarter_number = c.quarter_number
JOIN
    TopVehicleMaker tvm ON 1=1  -- Adding a dummy condition since we're selecting a single row
JOIN
    TopState ts ON 1=1  -- Adding a dummy condition since we're selecting a single row
ORDER BY o.quarter_number;



-- --------------------------------------------------------------------------------------------------------------------
-- Find Total values : Orders, revenue, average good feedback, average ship time, total customers

SELECT 
    SUM(quantity * o.vehicle_price) AS total_revenue,
    COUNT(*) AS total_orders,
    COUNT(DISTINCT customer_id) AS total_customers,
    AVG(DATEDIFF(ship_date, order_date)) AS average_days_to_ship,
    AVG(rating_number) AS average_rating,
    (COUNT(CASE
        WHEN customer_feedback IN ('Good' , 'Very Good') THEN 1
    END) / COUNT(*)) * 100 AS percent_good_feedback
FROM
    order_t o
        JOIN
    product_t p ON o.product_id = p.product_id
        LEFT JOIN
    (SELECT 
        product_id,
            CASE
                WHEN customer_feedback = 'Very Bad' THEN 1
                WHEN customer_feedback = 'Bad' THEN 2
                WHEN customer_feedback = 'Okay' THEN 3
                WHEN customer_feedback = 'Good' THEN 4
                WHEN customer_feedback = 'Very Good' THEN 5
                ELSE NULL
            END AS rating_number
    FROM
        order_t
    WHERE
        customer_feedback IN ('Very Bad' , 'Bad', 'Okay', 'Good', 'Very Good')) AS ratings ON o.product_id = ratings.product_id;



/*------------------------------------------Consistent performers------------------------------------------------------
Lets find those vehicle makers who has better consistency over quarterly performance with keeping some threshold of revenue 
*/

use newwheels;
WITH QuarterlyRevenue AS (
    SELECT
        vehicle_maker,
        quarter_number,
        SUM(quantity * order_t.vehicle_price) AS revenue,
        COUNT(order_t.order_id) AS orders
    FROM
        order_t
    JOIN
        product_t ON order_t.product_id = product_t.product_id
    GROUP BY
        vehicle_maker, quarter_number
),
QuarterlyChange AS (
    SELECT
        vehicle_maker,
        quarter_number,
        revenue,
        orders,
        LAG(revenue) OVER (PARTITION BY vehicle_maker ORDER BY quarter_number) AS prev_revenue,
        CASE
            WHEN LAG(revenue) OVER (PARTITION BY vehicle_maker ORDER BY quarter_number) = 0 THEN NULL
            ELSE ((revenue - LAG(revenue) OVER (PARTITION BY vehicle_maker ORDER BY quarter_number)) / NULLIF(LAG(revenue) OVER (PARTITION BY vehicle_maker ORDER BY quarter_number), 0)) * 100
        END AS revenue_change_percentage
    FROM
        QuarterlyRevenue
),
QuarterlyChangePivot AS (
    SELECT
        vehicle_maker,
        MAX(IF(quarter_number = 1, revenue, NULL)) AS Q1_revenue,
        MAX(IF(quarter_number = 1, orders, NULL)) AS Q1_orders,
        MAX(IF(quarter_number = 1, revenue_change_percentage, NULL)) AS Q1_revenue_change,
        MAX(IF(quarter_number = 2, revenue, NULL)) AS Q2_revenue,
        MAX(IF(quarter_number = 2, orders, NULL)) AS Q2_orders,
        MAX(IF(quarter_number = 2, revenue_change_percentage, NULL)) AS Q2_revenue_change,
        MAX(IF(quarter_number = 3, revenue, NULL)) AS Q3_revenue,
        MAX(IF(quarter_number = 3, orders, NULL)) AS Q3_orders,
        MAX(IF(quarter_number = 3, revenue_change_percentage, NULL)) AS Q3_revenue_change,
        MAX(IF(quarter_number = 4, revenue, NULL)) AS Q4_revenue,
        MAX(IF(quarter_number = 4, orders, NULL)) AS Q4_orders,
        MAX(IF(quarter_number = 4, revenue_change_percentage, NULL)) AS Q4_revenue_change
    FROM
        QuarterlyChange
    GROUP BY
        vehicle_maker
),
AverageQuarterlyChange AS (
    SELECT
        vehicle_maker,
        (COALESCE(Q1_revenue_change, 0) + COALESCE(Q2_revenue_change, 0) + COALESCE(Q3_revenue_change, 0) + COALESCE(Q4_revenue_change, 0)) /
        NULLIF(4 - (IFNULL(Q1_revenue_change, 0) = 0) + (IFNULL(Q2_revenue_change, 0) = 0) + (IFNULL(Q3_revenue_change, 0) = 0) + (IFNULL(Q4_revenue_change, 0) = 0), 0) AS avg_revenue_change
    FROM
        QuarterlyChangePivot
)
SELECT
    vehicle_maker,
    Q1_revenue,
    Q1_orders,
    Q1_revenue_change,
    Q2_revenue,
    Q2_orders,
    Q2_revenue_change,
    Q3_revenue,
    Q3_orders,
    Q3_revenue_change,
    Q4_revenue,
    Q4_orders,
    Q4_revenue_change,
    Q1_revenue + Q2_revenue + Q3_revenue + Q4_revenue AS total_revenue,
    Q1_orders + Q2_orders + Q3_orders + Q4_orders AS total_orders,
    avg_revenue_change
FROM
    QuarterlyChangePivot
JOIN
    AverageQuarterlyChange USING (vehicle_maker)
ORDER BY
    total_revenue DESC;

-- Top revenue generator States ----------------------------------------------------------------------------------------------------------------

SELECT
    customer_t.state,
    SUM(order_t.quantity * product_t.vehicle_price) AS total_revenue
FROM
    order_t
JOIN
    product_t ON order_t.product_id = product_t.product_id
JOIN
    customer_t ON order_t.customer_id = customer_t.customer_id
GROUP BY
    customer_t.state
ORDER BY
    total_revenue DESC;
 
-- --------------------------------------------------------Done----------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------------------



