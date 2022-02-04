from logging import setLoggerClass
import mysql.connector
from mysql.connector.constants import ServerFlag
import sys
import csv
import os.path
from typing import NoReturn
import sqlparse
import pprint

class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(user="Maxslide",password = "iiit123",host = "localhost",database = "QuarantinedAgain")
        self.cursor = self.home.cursor()

    def Create_temp_table(self, table_name, final_output, columns):
        # Need to decide how to name the joins etc
        table_name = "temp_something"
        # complete the syntax for create table
        creat_table = "CREATE TABLE "+table_name+" "
        self.cursor.execute(creat_table)
        for i in final_output:
            # complete as required by modifying the function a bit, we need to know the column names also which i dont think comes in the execute query output
            # need to figure this out
            insert_into_table = "INSERT INTO " +table_name+" VALUES"
        # At this point we have a temporary table created 
        return
    def execute_query(self,query):
        self.cursor.execute(query)
        # print(self.cursor)
        output_list = []
        for i in self.cursor:
            output_list.append(i)
        return output_list

obj = HomeDatabase()

# Join temp table -> Table_Name, Frag_Name