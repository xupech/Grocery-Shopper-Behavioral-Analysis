#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov  3 13:05:38 2019

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


'''
import data in block comment section

'''
DB_NAME = 'db_consumer_panel'

TABLES = {}
TABLES['Households'] = (
        "CREATE TABLE `Households` ("
        " `hh_id` INT(7) NOT NULL,"
        " `hh_race` INT NOT NULL,"
        " `hh_is_latinx` INT NOT NULL,"
        " `hh_income` FLOAT NOT NULL,"
        " `hh_size` INT NOT NULL,"
        " `hh_zip_code` INT(5) NOT NULL,"
        " `hh_state` VARCHAR(2) NOT NULL,"
        " `hh_residence_type` INT NOT NULL,"
        " PRIMARY KEY (`hh_id`)"
        " ) ENGINE = innoDB")

TABLES['Products'] = (
        "CREATE TABLE `Products` ("
        " `brand_at_prod_id` VARCHAR(100),"
        " `department_at_prod_id` VARCHAR(100),"
        " `prod_id` BIGINT NOT NULL,"
        " `group_at_prod_id` VARCHAR(100),"
        " `module_at_prod_id` VARCHAR(100),"
        " `amount_at_prod_id` FLOAT,"
        " `units_at_prod_id` VARCHAR(100),"
        " PRIMARY KEY (`prod_id`)"
        " ) ENGINE = innoDB")

TABLES['Trips'] = (
        "CREATE TABLE `Trips` ("
        " `hh_id` INT(7) NOT NULL,"
        " `TC_date` DATE,"
        " `TC_retailer_code` INT(3),"
        " `TC_retailer_code_store_code` INT(7),"
        " `TC_retailer_code_store_zip3` FLOAT,"
        " `TC_total_spent`FLOAT,"
        " `TC_id` BIGINT NOT NULL,"
        "  CONSTRAINT `Trips_ibfk_1` FOREIGN KEY (`hh_id`) "
        "     REFERENCES `Households` (`hh_id`) ON DELETE CASCADE,"
        " PRIMARY KEY (`TC_id`)"
        " ) ENGINE = innoDB")

TABLES['Purchases']=(
        "CREATE TABLE `Purchases` ("
        " `TC_id` BIGINT NOT NULL,"
        " `quantity_at_TC_prod_id` INT(3),"
        " `total_price_paid_at_TC_prod_id` FLOAT(7,2),"
        " `coupon_value_at_TC_prod_id` FLOAT(4,1),"
        " `deal_flag_at_TC_prod_id` FLOAT,"
        " `prod_id` BIGINT NOT NULL,"
        "  CONSTRAINT `Purchases_ibfk_1` FOREIGN KEY (`TC_id`) "
        "     REFERENCES `Trips` (`TC_id`) ON DELETE CASCADE,"
        "  CONSTRAINT `Purchases_ibfk_2` FOREIGN KEY (`prod_id`) "
        "     REFERENCES `Products` (`prod_id`) ON DELETE CASCADE,"
        "  PRIMARY KEY (`TC_id`,`prod_id`), KEY `Trips` (`TC_id`),"
    "  KEY `Products` (`prod_id`)"
        " ) ENGINE = innoDB")

mydb       = mysql.connector.connect(
  host     = "localhost",
  user     = "root",
  passwd   = "felizcumpleano",
)

cursor = mydb.cursor()

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        mydb.database = DB_NAME
    else:
        print(err)
        exit(1)

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
mydb.close()

engine = create_engine("mysql+mysqlconnector://root:felizcumpleano@localhost/db_consumer_panel")


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
'''
dta_at_hh.to_sql('Households', con = engine, if_exists = 'append', index=False, chunksize = 1000)

dta_at_prod_id.to_sql('Products', con = engine, if_exists = 'append', index=False, chunksize = 1000)

dta_at_TC.to_sql('Trips', con = engine, if_exists = 'append', index=False, chunksize = 1000)

dta_at_TC_upc.to_sql('Purchases', con = engine, if_exists = 'append', index=False, chunksize = 1000)