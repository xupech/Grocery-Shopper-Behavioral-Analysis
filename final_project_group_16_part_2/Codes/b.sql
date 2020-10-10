#B Code

#How many households do not shop at least once on a 3 month periods.
#(1)first assumption:
WITH t AS
(SELECT hh_id, TC_date, ROW_NUMBER() OVER (ORDER BY hh_id, TC_date) AS ID  FROM Trips order by hh_id,TC_date) 
     SELECT A.hh_id ,datediff(B.TC_date, A.TC_date) AS TIME_WINDOW_SIZE
     FROM t AS A INNER JOIN t AS B
     ON B.ID=A.ID + 1 AND B.hh_id=A.hh_id WHERE abs(datediff(A.TC_date,B.TC_date))>=90;
#(2)second assumption:
WITH 
  table1 AS (SELECT hh_id, row_number() over() +1 as id2, YEAR(TC_date) as year_before, MONTH(TC_date) AS month_before
			FROM Trips ORDER BY hh_id, year_before, month_before),
  table2 AS (SELECT hh_id, row_number() over() as id, YEAR(TC_date) as year_after, MONTH(TC_date) AS month_after
			 FROM Trips ORDER BY hh_id, year_after, month_after)
SELECT COUNT(DISTINCT table1.hh_id)
FROM table1 JOIN table2
WHERE table1.id2 = table2.id AND
   ((table2.year_after = table1.year_before AND table2.month_after - table1.month_before > 3)
    OR (table2.year_after = table1.year_before + 1 AND table1.month_before < 10));
     
#Loyalism: Among the households who shop at least once a month, which % of them
#concentrate at least 80% of their grocery expenditure (on average) on single retailer? 
#(1)first assumption:
WITH t AS
(SELECT hh_id, TC_date, ROW_NUMBER() OVER (ORDER BY hh_id, TC_date) AS ID  FROM Trips order by hh_id,TC_date) 
     SELECT DISTINCT(A.hh_id) 
     FROM t AS A INNER JOIN t AS B
     ON B.ID=A.ID + 1 AND B.hh_id=A.hh_id WHERE abs(datediff(A.TC_date,B.TC_date))<=30;

# 1)One one retailer
DROP TABLE IF EXISTS YY;
CREATE TABLE YY
SELECT A.hh_id, TC_retailer_code
FROM (SELECT hh_id, average FROM (SELECT hh_id, 0.8*SUM(TC_total_spent) AS average FROM Trips GROUP BY hh_id) AS B) AS A
CROSS JOIN (SELECT SUM(TC_total_spent) AS sum_by_retailer, TC_retailer_code, hh_id FROM Trips GROUP BY hh_id, TC_retailer_code ORDER BY hh_id) AS H 
ON A.hh_id=H.hh_id
WHERE H.sum_by_retailer > A.average;
SELECT * FROM YY;

# 2)Among Two
SELECT * 
FROM (SELECT P.hh_id, SUM(P.sum_by_retailer) AS sm
FROM (Select hh_id, sum_by_retailer, TC_retailer_code, row_number() over(PARTITION by hh_id ORDER BY sum_by_retailer desc) as r
from (SELECT hh_id, TC_retailer_code, SUM(TC_total_spent) AS sum_by_retailer FROM Trips GROUP BY hh_id, TC_retailer_code ORDER BY hh_id, sum_by_retailer DESC) AS U ORDER BY hh_id) AS P
WHERE P.r<3 
GROUP BY hh_id ORDER BY hh_id) AS PPP
CROSS JOIN 
(SELECT hh_id, 0.8*SUM(TC_total_spent) AS average FROM Trips GROUP BY hh_id) AS PPPP
ON PPP.hh_id= PPPP.hh_id
WHERE PPPP.average<PPP.sm;

#(2)second assumption:
WITH 
  table1 AS (SELECT hh_id, row_number() over() +1 as id2, YEAR(TC_date) as year_before, MONTH(TC_date) AS month_before
			FROM Trips ORDER BY hh_id, year_before, month_before),
  table2 AS (SELECT hh_id, row_number() over() as id, YEAR(TC_date) as year_after, MONTH(TC_date) AS month_after
			 FROM Trips ORDER BY hh_id, year_after, month_after)
SELECT COUNT(DISTINCT table1.hh_id)
FROM table1 JOIN table2
WHERE table1.id2 = table2.id AND
    ((table2.year_after = table1.year_before AND table2.month_after - table1.month_before <= 1)
    OR (table2.year_after = table1.year_before + 1 AND table1.month_before = 12 AND table2.month_after = 1));
    
# 1)One one retailer
WITH 
  table1 AS (SELECT D.id AS ID, D.num_retailer AS number_retailer
             FROM (SELECT C.ID AS id, COUNT(C.retailer_ID) AS num_retailer
	               FROM (SELECT A.hh_id AS ID, A.TC_retailer_code AS retailer_ID
                         FROM (SELECT hh_id, TC_retailer_code, SUM(TC_total_spent) AS spent_per_retailer
                               FROM Trips
                               GROUP BY hh_id, TC_retailer_code) AS A
                         LEFT JOIN (SELECT hh_id, 0.8 * SUM(TC_total_spent) AS spent08
                                    FROM Trips
                                    GROUP BY hh_id) AS B
                         ON A.hh_id = B.hh_id
                         WHERE A.spent_per_retailer > B.spent08) AS C
             GROUP BY C.ID) AS D 
             WHERE D.num_retailer = 1),
  table2 AS (SELECT DISTINCT A.hh_id AS ID
             FROM (SELECT hh_id, row_number() over() +1 as id2, YEAR(TC_date) as year_before, MONTH(TC_date) AS month_before
	               FROM Trips ORDER BY hh_id, year_before, month_before) AS A
             JOIN (SELECT hh_id, row_number() over() as id, YEAR(TC_date) as year_after, MONTH(TC_date) AS month_after
	               FROM Trips ORDER BY hh_id, year_after, month_after) AS B
             ON A.id2 = B.id
             WHERE ((B.year_after = A.year_before AND B.month_after - A.month_before <= 1)
                   OR (B.year_after = A.year_before + 1 AND A.month_before = 12 AND B.month_after = 1)))
SELECT COUNT(DISTINCT table1.ID)
FROM table1 JOIN table2
WHERE table1.ID = table2.ID;

# 2)Among Two
WITH 
  table1 AS (SELECT D.hh_id AS hh_id, D.top2_spent AS top2_spent, E.spent08 AS spent08
             FROM (SELECT C.hh_id AS hh_id, SUM(C.spent_per_retailer) AS top2_spent
                   FROM (SELECT B.hh_id AS hh_id, B.TC_retailer_code AS TC_retailer_code, B.spent_per_retailer AS spent_per_retailer, B.rank_spent_retailer AS rank_spent_retailer
                         FROM (SELECT A.hh_id AS hh_id, A.TC_retailer_code AS TC_retailer_code, A.spent_per_retailer AS spent_per_retailer,rank () OVER (PARTITION BY A.hh_id ORDER BY A.spent_per_retailer DESC) AS rank_spent_retailer
                               FROM (SELECT hh_id, TC_retailer_code, SUM(TC_total_spent) AS spent_per_retailer
									 FROM Trips
                                     GROUP BY hh_id, TC_retailer_code
                                     ORDER BY hh_id, spent_per_retailer DESC) AS A) AS B
						 WHERE B.rank_spent_retailer IN (1,2)) AS C
                  GROUP BY C.hh_id) AS D
            LEFT JOIN (SELECT hh_id, 0.8 * SUM(TC_total_spent) AS spent08
                       FROM Trips 
                       GROUP BY hh_id) AS E
            ON D.hh_id = E.hh_id
            WHERE D.top2_spent > E.spent08),
  table2 AS (SELECT DISTINCT A.hh_id AS hh_id
             FROM (SELECT hh_id, row_number() over() +1 as id2, YEAR(TC_date) as year_before, MONTH(TC_date) AS month_before
	               FROM Trips ORDER BY hh_id, year_before, month_before) AS A
             JOIN (SELECT hh_id, row_number() over() as id, YEAR(TC_date) as year_after, MONTH(TC_date) AS month_after
	               FROM Trips ORDER BY hh_id, year_after, month_after) AS B
             ON A.id2 = B.id
             WHERE ((B.year_after = A.year_before AND B.month_after - A.month_before <= 1)
                   OR (B.year_after = A.year_before + 1 AND A.month_before = 12 AND B.month_after = 1)))
SELECT COUNT(DISTINCT table1.hh_id)
FROM table1 JOIN table2
WHERE table1.hh_id = table2.hh_id;

#i. Are their demographics remarkably different? Are these people richer? Poorer?
#(1)Income Distribution
# 1)Among Two 
SELECT COUNT(H.hh_id), H.hh_income
FROM Households AS H
CROSS JOIN (SELECT OO.hh_id
FROM T AS OO
INNER JOIN 
A AS OOO
ON OOO.hh_id=OO.hh_id) AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_income ORDER BY H.hh_income;
# 2)One Retailer
SELECT COUNT(H.hh_id), H.hh_income
FROM Households AS H
CROSS JOIN YY AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_income ORDER BY H.hh_income;

#Family Member Distribution
# 1)Among Two
SELECT COUNT(H.hh_id), H.hh_size
FROM Households AS H
CROSS JOIN (SELECT OO.hh_id
FROM T AS OO
INNER JOIN 
A AS OOO
ON OOO.hh_id=OO.hh_id) AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_size ORDER BY H.hh_size;
# 2)One retailer
SELECT COUNT(H.hh_id), H.hh_size
FROM Households AS H
CROSS JOIN YY AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_size ORDER BY H.hh_size;

#ii. What is the retailer that has more loyalists?
With t AS 
(SELECT hh_id, average
FROM (SELECT hh_id, 0.8*SUM(TC_total_spent) AS average FROM Trips GROUP BY hh_id) AS B)
SELECT H.TC_retailer_code, count(H.hh_id)
FROM t AS A CROSS JOIN (SELECT SUM(TC_total_spent) AS sum_by_retailer, TC_retailer_code, hh_id FROM Trips GROUP BY hh_id, TC_retailer_code ORDER BY hh_id) AS H 
ON A.hh_id=H.hh_id
WHERE H.sum_by_retailer > A.average
GROUP BY TC_retailer_code ORDER BY TC_retailer_code;

SELECT P.TC_retailer_code, count(P.hh_id)
FROM (Select hh_id, sum_by_retailer, TC_retailer_code, row_number() over(PARTITION by hh_id ORDER BY sum_by_retailer desc) as r
from (SELECT hh_id, TC_retailer_code, SUM(TC_total_spent) AS sum_by_retailer FROM Trips GROUP BY hh_id, TC_retailer_code ORDER BY hh_id, sum_by_retailer DESC) AS U ORDER BY hh_id) AS P
WHERE P.r<3 
GROUP BY TC_retailer_code ORDER BY TC_retailer_code;

#iii. Where do they live? Plot the distribution by state.
#(1)State Distribution
# 1)Among Two
SELECT COUNT(H.hh_id), H.hh_state
FROM Households AS H
CROSS JOIN (SELECT OO.hh_id
FROM T AS OO
INNER JOIN 
A AS OOO
ON OOO.hh_id=OO.hh_id) AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_state ORDER BY H.hh_state;
# 2)One Retailer
SELECT COUNT(H.hh_id), H.hh_state
FROM Households AS H
CROSS JOIN YY AS G
ON H.hh_id=G.hh_id 
GROUP BY H.hh_state ORDER BY H.hh_state;

#i. Average number of items purchased on a given month.
SELECT  B.mon,  SUM(C.quantity_at_TC_prod_id)/COUNT(DISTINCT(B.hh_id))
FROM(SELECT hh_id,DATE_FORMAT(TC_date, '%Y-%m') AS mon, TC_id FROM Trips) AS B
CROSS JOIN(SELECT TC_id, quantity_at_TC_prod_id FROM Purchases) AS C
ON B.TC_id=C.TC_id
GROUP BY B.mon ORDER BY B.mon;

#ii. Average number of shopping trips per month.
SELECT DATE_FORMAT(TC_date, '%Y-%m') AS mon, COUNT(TC_id)/COUNT(DISTINCT(hh_id)) FROM Trips GROUP BY mon ORDER BY mon;

#iii. Average number of days between 2 consecutive shopping trips.
WITH t AS
(SELECT hh_id, TC_date, ROW_NUMBER() OVER (ORDER BY hh_id, TC_date) AS ID  FROM Trips order by hh_id,TC_date) 
     SELECT A.hh_id ,AVG(datediff(B.TC_date, A.TC_date)) AS TIME_WINDOW_SIZE
     FROM t AS A INNER JOIN t AS B
     ON B.ID=A.ID + 1 AND B.hh_id=A.hh_id
     GROUP BY A.hh_id;