#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 31 11:17:27 2019

@author: hsiangyao
"""

import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import csv


mydb= mysql.connector.connect(
host= "localhost",
user= "root",
password= "yao19940928",
database= "db_consumer_panel")

print(mydb)

mycursor = mydb.cursor(buffered=True)
mycursor.execute("show tables;")
mycursor.close()
# In[]
#Import State Data
for x in mycursor:   print(x)

sql_query = "SELECT COUNT(H.hh_id), H.hh_state FROM Households AS H CROSS JOIN YY AS G  ON H.hh_id=G.hh_id GROUP BY H.hh_state ORDER BY H.hh_state;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
myresult = mycursor.fetchall()
mycursor.close()


# In[]
# Import State Data
sql_query = "SELECT COUNT(H.hh_id), H.hh_state FROM Households AS H CROSS JOIN (SELECT OO.hh_id FROM T AS OO INNER JOIN  A AS OOO ON OOO.hh_id=OO.hh_id) AS G ON H.hh_id=G.hh_id GROUP BY H.hh_state ORDER BY H.hh_state"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
state_more_dis = mycursor.fetchall()
mycursor.close()
# In[]
def plot_timeseries(axes, x, y, color, xlabel, ylabel,linestyle):

  axes.plot(x, y, color=color, linestyle=linestyle)
  axes.set_xlabel(xlabel)
  axes.set_ylabel(ylabel, color=color)
  axes.tick_params('y', colors=color)

# In[]
#State Distribution
state_more_dis_data=pd.DataFrame(data=state_more_dis)
myresult_data=pd.DataFrame(data=myresult)
fig, ax = plt.subplots(sharey=True,figsize=(15, 8))
ax.plot(state_more_dis_data[1], state_more_dis_data[0],color='sienna', marker='o',linestyle='--')
ax2 = ax.twinx()
ax2.plot(myresult_data[1], myresult_data[0],color='sandybrown', marker='v',linestyle='--')
ax.set_xlabel("STATE")
ax.set_ylabel("Numbers of households")
ax2.set_ylabel("Numbers of households")
ax.tick_params('Numbers of households', colors='sienna')
ax2.tick_params('Numbers of households', colors='sandybrown')
plot_timeseries(ax, state_more_dis_data[1], state_more_dis_data[0], "sienna", "STATE", "Numbers of households", "--")
plot_timeseries(ax2, myresult_data[1], myresult_data[0], "sandybrown", "STATE", "Numbers of households", "--")


# In[]
#Import Income Data
sql_query = "SELECT COUNT(H.hh_id), H.hh_income FROM Households AS H CROSS JOIN (SELECT OO.hh_id FROM T AS OO INNER JOIN  A AS OOO ON OOO.hh_id=OO.hh_id) AS G ON H.hh_id=G.hh_id  GROUP BY H.hh_income ORDER BY H.hh_income;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
income_second = mycursor.fetchall()
mycursor.close()
# In[]
# Import Income Data
sql_query = "SELECT COUNT(H.hh_id), H.hh_income FROM Households AS H CROSS JOIN YY AS G ON H.hh_id=G.hh_id  GROUP BY H.hh_income ORDER BY H.hh_income;) AS G ON H.hh_id=G.hh_id  GROUP BY H.hh_income ORDER BY H.hh_income;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
income_first = mycursor.fetchall()
mycursor.close()
# In[]
#Income Distribution
income_first_data=pd.DataFrame(data=income_first)
income_second_data=pd.DataFrame(data=income_second)
fig, ax = plt.subplots(sharey=True)
ax.plot(income_first_data[1], income_first_data[0],color='cornflowerblue', marker='o',linestyle='--',linewidth=2)
ax2 = ax.twinx()
ax2.plot(income_second_data[1], income_second_data[0],color='royalblue', marker='v',linestyle='--',linewidth=2)
ax.set_title("Income Level Distribution", color="darkcyan")
ax.set_xlabel("Income Level")
ax.set_ylabel("Numbers of households")
ax2.set_ylabel("Numbers of households")


plot_timeseries(ax, income_first_data[1], income_first_data[0], "cornflowerblue", "Income Level", "Numbers of households", "--")
plot_timeseries(ax2, income_second_data[1], income_second_data[0], "blue", "Income Level", "Numbers of households", "--")

# In[]
# Import Family Members Data
sql_query = "SELECT COUNT(H.hh_id), H.hh_size FROM Households AS H CROSS JOIN YY AS G ON H.hh_id=G.hh_id GROUP BY H.hh_size ORDER BY H.hh_size;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
family_first = mycursor.fetchall()
mycursor.close()

# In[]
#Import Family Members Data
sql_query = "SELECT COUNT(H.hh_id), H.hh_size FROM Households AS H CROSS JOIN (SELECT OO.hh_id FROM T AS OO INNER JOIN  A AS OOO ON OOO.hh_id=OO.hh_id) AS G ON H.hh_id=G.hh_id  GROUP BY H.hh_size ORDER BY H.hh_size;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
family_second = mycursor.fetchall()
mycursor.close()


# In[]
#Family Members Distribution
family_first_data=pd.DataFrame(data=family_first)
family_second_data=pd.DataFrame(data=family_second)
fig, ax = plt.subplots(sharey=True)
ax.plot(family_first_data[1], family_first_data[0],color='cornflowerblue', marker='o',linestyle='--',linewidth=2)
ax2 = ax.twinx()
ax2.plot(family_second_data[1], family_second_data[0],color='blue', marker='v',linestyle='--',linewidth=2)
ax.set_title("House Members Distribution", color="darkcyan")
ax.set_xlabel("Number Of Members")
ax.set_ylabel("Number of households")
ax2.set_ylabel("Number of households")
plot_timeseries(ax, family_first_data[1], family_first_data[0], "cornflowerblue", "Number Of Members", "Number of households", "--")
plot_timeseries(ax2, family_second_data[1], family_second_data[0], "blue", "Number Of Members", "Number of households", "--")


# In[]
#i.	Average number of items purchased on a given month. 
sql_query = "SELECT  B.mon,  SUM(C.quantity_at_TC_prod_id)/COUNT(DISTINCT(B.hh_id)) AS average_products FROM(SELECT hh_id,DATE_FORMAT(TC_date, '%Y-%m') AS mon, TC_id FROM Trips) AS B CROSS JOIN(SELECT TC_id, quantity_at_TC_prod_id FROM Purchases) AS C ON B.TC_id=C.TC_id GROUP BY B.mon ORDER BY B.mon;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
items_purchased = mycursor.fetchall()
mycursor.close()


# In[]
#i.	Average number of items purchased on a given month. 
items_purchased_data=pd.DataFrame(data=items_purchased)
fig, ax = plt.subplots(sharey=True)
#items_purchased_data[0]=("2003-12","2004-01","2004-02","2004-03","2004-04","2004-05","2004-06","2004-07","2004-08","2004-09","2004-10","2004-11","2004-12")
#items_purchased_data[0]=("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
ax.bar(items_purchased_data[0], items_purchased_data[1],color='dodgerblue')
ax.plot(items_purchased_data[0], items_purchased_data[1],color='darkblue', marker='o',linestyle='--')
ax.set_xticklabels(items_purchased_data[0], rotation=90)
ax.set_title("Average Items Purchased", color="darkcyan")
ax.set_xlabel("Month")
ax.set_ylabel("Item Purchased")
ax.tick_params('Item Purchased', color='darkviolet')



# In[]
#ii. Average number of shopping trips per month.
sql_query = "SELECT DATE_FORMAT(TC_date, '%Y-%m') AS mon, COUNT(TC_id)/COUNT(DISTINCT(hh_id)) FROM Trips GROUP BY mon ORDER BY mon;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
average_trip = mycursor.fetchall()
mycursor.close()



# In[]
#ii. Average number of shopping trips per month.
average_trip_data=pd.DataFrame(data=average_trip)
fig, ax = plt.subplots(sharey=True)
ax.plot(average_trip_data[0], average_trip_data[1], "darkviolet", linestyle="--", marker='o')
ax.bar(average_trip_data[0],average_trip_data[1], color='skyblue')
ax.set_title("Average Shopping Trips Per Month", color="darkcyan")
ax.set_xticklabels(average_trip_data[0], rotation=90)
plot_timeseries(ax, average_trip_data[0], average_trip_data[1], "darkviolet", "Month", "Average number of Shopping Trips", "--")


# In[]
#iii. Average number of days between 2 consecutive shopping trips.
sql_query = "WITH t AS (SELECT hh_id, TC_date, ROW_NUMBER() OVER (ORDER BY hh_id, TC_date) AS ID  FROM Trips order by hh_id,TC_date) SELECT A.hh_id ,AVG(datediff(B.TC_date, A.TC_date)) AS TIME_WINDOW_SIZE FROM t AS A INNER JOIN t AS B ON B.ID=A.ID + 1 AND B.hh_id=A.hh_id GROUP BY A.hh_id;"
mycursor = mydb.cursor()
mycursor.execute(sql_query)
two_con_shopping = mycursor.fetchall()
mycursor.close()
two_con_shopping
two_con_shopping_data=pd.DataFrame(data=two_con_shopping)


# In[]
#iii. Average number of days between 2 consecutive shopping trips.
fig, ax = plt.subplots()
ax.scatter(two_con_shopping_data[0],two_con_shopping_data[1],c=two_con_shopping_data[0])
#plt.ylim(0,15)
ax.set_xticklabels(two_con_shopping_data[0], rotation=90)
ax.set_xlabel("Households")
ax.set_ylabel("Average Days Of Consecutive Shopping Trips")
ax.set_title("Average Days Of Consecutive Shopping Trips", color="darkcyan")
plt.show()



