#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 25 14:43:33 2019

@author: xupech
"""

import pandas as pd
from pandas import DataFrame
import numpy as np
import matplotlib.pyplot as plt
from scipy import stats
from sklearn.cluster import KMeans
import seaborn as sns
import mysql.connector
from mysql.connector import errorcode
import csv
import os
from sqlalchemy import create_engine


# =============================================================================
# '''
# import data in block comment section
# 
# '''
# DB_NAME = 'db_consumer_panel'
# 
# TABLES = {}
# TABLES['Households'] = (
#         "CREATE TABLE `Households` ("
#         " `hh_id` INT(7) NOT NULL,"
#         " `hh_race` INT NOT NULL,"
#         " `hh_is_latinx` INT NOT NULL,"
#         " `hh_income` FLOAT NOT NULL,"
#         " `hh_size` INT NOT NULL,"
#         " `hh_zip_code` INT(5) NOT NULL,"
#         " `hh_state` VARCHAR(2) NOT NULL,"
#         " `hh_residence_type` INT NOT NULL,"
#         " PRIMARY KEY (`hh_id`)"
#         " ) ENGINE = innoDB")
# 
# TABLES['Products'] = (
#         "CREATE TABLE `Products` ("
#         " `brand_at_prod_id` VARCHAR(100),"
#         " `department_at_prod_id` VARCHAR(100),"
#         " `prod_id` BIGINT NOT NULL,"
#         " `group_at_prod_id` VARCHAR(100),"
#         " `module_at_prod_id` VARCHAR(100),"
#         " `amount_at_prod_id` FLOAT,"
#         " `units_at_prod_id` VARCHAR(100),"
#         " PRIMARY KEY (`prod_id`)"
#         " ) ENGINE = innoDB")
# 
# TABLES['Trips'] = (
#         "CREATE TABLE `Trips` ("
#         " `hh_id` INT(7) NOT NULL,"
#         " `TC_date` DATE,"
#         " `TC_retailer_code` INT(3),"
#         " `TC_retailer_code_store_code` INT(7),"
#         " `TC_retailer_code_store_zip3` FLOAT,"
#         " `TC_total_spent`FLOAT,"
#         " `TC_id` BIGINT NOT NULL,"
#         "  CONSTRAINT `Trips_ibfk_1` FOREIGN KEY (`hh_id`) "
#         "     REFERENCES `Households` (`hh_id`) ON DELETE CASCADE,"
#         " PRIMARY KEY (`TC_id`)"
#         " ) ENGINE = innoDB")
# 
# TABLES['Purchases']=(
#         "CREATE TABLE `Purchases` ("
#         " `TC_id` BIGINT NOT NULL,"
#         " `quantity_at_TC_prod_id` INT(3),"
#         " `total_price_paid_at_TC_prod_id` FLOAT(7,2),"
#         " `coupon_value_at_TC_prod_id` FLOAT(4,1),"
#         " `deal_flag_at_TC_prod_id` FLOAT,"
#         " `prod_id` BIGINT NOT NULL,"
#         "  CONSTRAINT `Purchases_ibfk_1` FOREIGN KEY (`TC_id`) "
#         "     REFERENCES `Trips` (`TC_id`) ON DELETE CASCADE,"
#         "  CONSTRAINT `Purchases_ibfk_2` FOREIGN KEY (`prod_id`) "
#         "     REFERENCES `Products` (`prod_id`) ON DELETE CASCADE,"
#         "  PRIMARY KEY (`TC_id`,`prod_id`), KEY `Trips` (`TC_id`),"
#     "  KEY `Products` (`prod_id`)"
#         " ) ENGINE = innoDB")
# 
# mydb       = mysql.connector.connect(
#   host     = "localhost",
#   user     = "root",
#   passwd   = "felizcumpleano",
# )
# 
# cursor = mydb.cursor()
# 
# def create_database(cursor):
#     try:
#         cursor.execute(
#             "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
#     except mysql.connector.Error as err:
#         print("Failed creating database: {}".format(err))
#         exit(1)
# 
# 
# try:
#     cursor.execute("USE {}".format(DB_NAME))
# except mysql.connector.Error as err:
#     print("Database {} does not exists.".format(DB_NAME))
#     if err.errno == errorcode.ER_BAD_DB_ERROR:
#         create_database(cursor)
#         print("Database {} created successfully.".format(DB_NAME))
#         mydb.database = DB_NAME
#     else:
#         print(err)
#         exit(1)
# 
# for table_name in TABLES:
#     table_description = TABLES[table_name]
#     try:
#         print("Creating table {}: ".format(table_name), end='')
#         cursor.execute(table_description)
#     except mysql.connector.Error as err:
#         if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
#             print("already exists.")
#         else:
#             print(err.msg)
#     else:
#         print("OK")
# 
# cursor.close()
# mydb.close()
# 
# engine = create_engine("mysql+mysqlconnector://root:felizcumpleano@localhost/db_consumer_panel")
# =============================================================================


#def open_csv_file(path):
#    with open(path, 'r') as csvfile:
#        csvreader = csv.reader(csvfile)
#        return csvreader

dta_at_hh = pd.read_csv(r'/Users/xupech/Desktop/brandeis graduate school/Academics/2019 FALL/BUS 211 Analyzing Big Data/Final Project/dta_at_hh.csv')
dta_at_hh.drop('Unnamed: 0', axis=1, inplace=True)

'''
 ['hh_id' 'hh_race' 'hh_is_latinx' 'hh_zip_code' 'hh_state' 'hh_income'
 'hh_size' 'hh_residence_type']
'''



dta_at_prod_id= pd.read_csv(r'/Users/xupech/Desktop/brandeis graduate school/Academics/2019 FALL/BUS 211 Analyzing Big Data/Final Project/dta_at_prod_id.csv')
dta_at_prod_id.drop('Unnamed: 0', axis=1, inplace=True)
dta_at_prod_id= dta_at_prod_id.drop_duplicates(subset=['prod_id']) 


'''
['brand_at_prod_id' 'department_at_prod_id' 'prod_id' 'group_at_prod_id'
 'module_at_prod_id' 'amount_at_prod_id' 'units_at_prod_id']
'''

dta_at_TC = pd.read_csv(r'/Users/xupech/Desktop/brandeis graduate school/Academics/2019 FALL/BUS 211 Analyzing Big Data/Final Project/dta_at_TC.csv')
dta_at_TC.drop('Unnamed: 0', axis=1, inplace=True)
dta_at_TC= dta_at_TC.drop_duplicates(subset=['TC_id']) 
'''
['hh_id' 'TC_date' 'TC_retailer_code' 'TC_retailer_code_store_code'
 'TC_retailer_code_store_zip3' 'TC_total_spent' 'TC_id']
'''


dta_at_TC_upc = pd.read_csv(r'/Users/xupech/Desktop/brandeis graduate school/Academics/2019 FALL/BUS 211 Analyzing Big Data/Final Project/dta_at_TC_upc.csv')
dta_at_TC_upc.drop('Unnamed: 0', axis=1, inplace=True)
dta_at_TC_upc = dta_at_TC_upc.drop_duplicates(subset=['TC_id','prod_id'])
print(dta_at_TC_upc.columns.values)

'''
['TC_id' 'quantity_at_TC_prod_id' 'total_price_paid_at_TC_prod_id'
 'coupon_value_at_TC_prod_id' 'deal_flag_at_TC_prod_id' 'prod_id']
# =============================================================================
# '''
# dta_at_hh.to_sql('Households', con = engine, if_exists = 'append', index=False, chunksize = 1000)
# 
# dta_at_prod_id.to_sql('Products', con = engine, if_exists = 'append', index=False, chunksize = 1000)
# 
# dta_at_TC.to_sql('Trips', con = engine, if_exists = 'append', index=False, chunksize = 1000)
# 
# dta_at_TC_upc.to_sql('Purchases', con = engine, if_exists = 'append', index=False, chunksize = 1000)
# =============================================================================

'''
query

'''
mydb       = mysql.connector.connect(
   host     = "localhost",
   user     = "root",
   passwd   = "felizcumpleano", database='db_consumer_panel'
)

cursor = mydb.cursor(buffered = True)

#a.i
query0 = ("SELECT COUNT(TC_id) AS `number_of_trips` FROM Trips")
cursor.execute(query0)
resulta = cursor.fetchall()

print("Total number of trips: " + str(resulta[0][0]))

"""
Total number of trips: 7596145.

"""
#a.ii
query1 = ("SELECT COUNT(hh_id) FROM Households")
cursor.execute(query1)
resultb = cursor.fetchall()
print("Total number of Households: " + str(resultb[0][0]))

"""
Total number of Households: 39577.

"""

#a.iii
query2 = ("SELECT COUNT(DISTINCT TC_retailer_code) FROM Trips")
cursor.execute(query2)
resultc = cursor.fetchall()
print("Total number of retailers: " + str(resultc[0][0]))

#a.iv[1.1]
query3 = ("select group_at_prod_id, COUNT(prod_id)from Products GROUP BY group_at_prod_id")
cursor.execute(query3)
resultd = cursor.fetchall()
for result in resultd:
    print(str(result[0]) + '   '+str(result[1]))

#a.iv[1.2]
query8 = ("select module_at_prod_id, COUNT(prod_id)from Products GROUP BY module_at_prod_id")
cursor.execute(query8)
resulti = cursor.fetchall()
for result in resulti:
    print(str(result[0]) + '   '+str(result[1]))
    
#a.iv[2.1]
query4 = ("select department_at_prod_id, COUNT(prod_id) FROM Products GROUP BY department_at_prod_id")
cursor.execute(query4)
resulte = cursor.fetchall()
print(resulte)

c = resulte.copy()
f = ()
for i in c:
    if i[0] == None:
        f = ('None', i[1])
        resulte.append(f)
        resulte.remove(i)

resulte.sort(key=lambda x:x[1])

        
        
depart, num_product=zip(*resulte)

fig, ax = plt.subplots()
index = np.arange(len(depart))
bar_width = 0.55
opacity = 0.8


chart0 = ax.barh(index, num_product, bar_width, align = 'center', alpha=0.5)
for i, v in enumerate(num_product):
    ax.text(v+3, i, str(v), color='blue')


plt.yticks(index, depart)
plt.ylabel('department')
plt.title('Products per Department')
fig.savefig('Products per Department')
plt.show()


#a.iv[2.2]
query5 = ("select department_at_prod_id, COUNT(module_at_prod_id) FROM Products GROUP BY department_at_prod_id")
cursor.execute(query5)
resultf = cursor.fetchall()
print(resultf)

d = resultf.copy()
g = ()
for i in d:
    if i[0] == None:
        g = ('None', i[1])
        resultf.append(g)
        resultf.remove(i)
        
resultf.sort(key=lambda x:x[1])


depart, num_module=zip(*resultf)

fig, ax = plt.subplots()
index = np.arange(len(depart))
bar_width = 0.55
opacity = 0.8

chart0 = ax.barh(index, num_module, bar_width, align = 'center', alpha=0.5)
for i, v in enumerate(num_module):
    ax.text(v+3, i, str(v), color='blue')

plt.yticks(index, depart)
plt.ylabel('department')
plt.title('Modules per Department')

plt.show()
fig.savefig('Modules per Department')

# a.v[1]
query6 = ("select COUNT(TC_id) FROM Purchases WHERE coupon_value_at_TC_prod_id = 0")
cursor.execute(query6)
resultg = cursor.fetchall()
print(resultg)

# a.v[2]
query7 = ("select COUNT(TC_id) FROM Purchases WHERE coupon_value_at_TC_prod_id > 0")
cursor.execute(query7)
resulth = cursor.fetchall()
print(resulth)

#c(ii)
dta_at_TC_upc['average']=dta_at_TC_upc.apply(lambda row: row.total_price_paid_at_TC_prod_id/row.quantity_at_TC_prod_id, axis=1)
corr=dta_at_TC_upc['average'].corr(dta_at_TC_upc['quantity_at_TC_prod_id'])


plt.style.use("seaborn")
fig, ax=plt.subplots()
ax.scatter(dta_at_TC_upc['average'], dta_at_TC_upc['quantity_at_TC_prod_id'])
plt.title('Correlation between average price and quantity of products bought')
plt.show()


#c(iii)(3)

incomeframe = dta_at_hh[['hh_id','hh_income']].copy()
kmeans = KMeans(n_clusters=3).fit(incomeframe['hh_income'].values.reshape(-1,1))
labels = kmeans.labels_
incomeframe['clusters']=labels
firstquantile = incomeframe.loc[incomeframe['clusters'] == 0]
secondquantile = incomeframe.loc[incomeframe['clusters'] == 1]
thirdquantile = incomeframe.loc[incomeframe['clusters'] == 2]


trips = dta_at_TC[['hh_id', 'TC_date', 'TC_total_spent', 'TC_id']].copy()
trips['date']=pd.to_datetime(trips['TC_date'])

maxdate= trips['date'].max()
mindate= trips['date'].min()

total=pd.DataFrame(trips.groupby(['hh_id'])['TC_total_spent'].sum())

high=firstquantile.join(total, on='hh_id', how='left')
medium=secondquantile.join(total, on='hh_id', how='left')
low=thirdquantile.join(total, on='hh_id', how='left')

high['average']=round(high['TC_total_spent']/12,2)
medium['average']=round(medium['TC_total_spent']/12,2)     
low['average']=round(low['TC_total_spent']/12,2)

temp0=dta_at_TC_upc[['TC_id','total_price_paid_at_TC_prod_id','prod_id']].copy()
dta_at_prod_id.prod_id = dta_at_prod_id.prod_id.astype(int)
temp1= dta_at_prod_id[['prod_id','brand_at_prod_id']].copy()
temp2=temp0.merge(temp1, on='prod_id', how='left')

ctlbr=pd.DataFrame(temp2.loc[temp2['brand_at_prod_id']=='CTL BR'])
totalctlbr=pd.DataFrame(ctlbr.groupby(['TC_id'])['total_price_paid_at_TC_prod_id'].sum())
temp3=totalctlbr.merge(trips, on='TC_id', how='left')
total1=pd.DataFrame(temp3.groupby(['hh_id'])['total_price_paid_at_TC_prod_id'].sum())

high1=high.join(total1, on='hh_id', how='left')
medium1=medium.join(total1, on='hh_id', how='left')
low1=low.join(total1, on='hh_id', how='left')

high1['averagectlbr']=round(high1['total_price_paid_at_TC_prod_id']/12,2)
medium1['averagectlbr']=round(medium1['total_price_paid_at_TC_prod_id']/12,2)     
low1['averagectlbr']=round(low1['total_price_paid_at_TC_prod_id']/12,2)

high1['ctlbrpercentage']=high1['averagectlbr']/high1['average']
medium1['ctlbrpercentage']=medium1['averagectlbr']/medium1['average']       
low1['ctlbrpercentage']=low1['averagectlbr']/low1['average'] 

high1m=   high1['ctlbrpercentage'].mean()    
medium1m=medium1['ctlbrpercentage'].mean()
low1m=low1['ctlbrpercentage'].mean()

afterall=pd.concat([high1, medium1, low1])
afterallnew=afterall[['hh_income', 'ctlbrpercentage']].copy()
    
afterallnew.fillna(0)   

#ax.scatter(afterallnew['hh_income'], afterall['ctlbrpercentage'])

#sns.regplot(afterallnew['hh_income'], afterall['ctlbrpercentage'])
#plt.show()

sns_plot=sns.jointplot(x=afterallnew['hh_income'], y=afterall['ctlbrpercentage'], kind='reg',
                  joint_kws={'line_kws':{'color':'cyan'}})
sns_plot.savefig("ctblrpercentage_vs_income")

        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        