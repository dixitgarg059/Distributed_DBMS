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

temp_tables = []
queries = []
num = last_node_number

def Key_col(table):
    query = "Select Frag_Condition from Frag_Table where Frag_Name = '" + table + "';"
    out = obj.execute(query)


def Frag_Type(table):
    query = "Select Frag_Type from Tables where Table_Name = '" + table + "';"
    out = obj.execute(query)
    if(out[0].strip()=="DHF" or out[0].strip()=="HF"):
        return True
    else:
        return False

def create_graph(n,edges):
    adj = [[0]*n]*n
    vis = [[0]*n]
    for edge in edges:
        u = edge[0]
        v = edge[1]
        adj[u][v] = 1
        adj[v][u] = 1
    return adj,vis

def dfs(n):
    opt = nodes[n]['Key']
    if opt == 'Table_Fragment':
        vis[n] = 1
        return nodes[n]
    
    elif opt == 'Union_Frag':
        child = []
        vis[n]=1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        if(Frag_Type(child[0]['Table_Name'])):
            query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
            select = 'Select * From '
            where = ' Where '
            union = ' Union '
            And = ' And '
            
            for i in range(len(child)-1):
                query += select
                frag_name = child[i]['Value']
                frag_cond = child[i]['Condition']
                query += frag_name 
                cond = len(frag_cond)
                if cond:
                    query += where
                    for j in range(cond-1):
                        query += frag_cond[j] 
                        query += And
                    query += frag_cond[cond-1]
                query += union
                
            i = len(child)-1
            query += select
            frag_name = child[i]['Value']
            frag_cond = child[i]['Condition']
            query += frag_name 
            cond = len(frag_cond)
            if cond:
                query += where
                for j in range(cond-1):
                    query += frag_cond[j] 
                    query += And
                query += frag_cond[cond-1]

            query += ');'
        
        else:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
            inner_join = ' Inner Join '
            on = ' ON '
            equal = ' = '
            key_col = Key_col(child[0]['Value'])

            for i in range(1,len(child)):
                query += '('
            
            query += child[0]['Value'] + inner_join + child[1]['Value'] + on 
            query += child[0]['Value'] + '.' + key_col + equal + child[1]['Value'] + '.' +key_col

            for i in range(2,len(child)):
                query += ')' + inner_join + child[i]['Value'] + on
                query += child[0]['Value'] + '.' + key_col + equal + child[i]['Value'] + '.' +key_col
            
            query += ');'
        
        temp_tables.append(nodes[n]['Value'])
        queries.append(query)
        return nodes[n]
        
    elif opt == 'Union':
        child = []
        vis[n]=1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
        select = 'Select * From '
        where = ' Where '
        union = ' Union '
        And = ' And '
        
        for i in range(len(child)-1):
            query += select
            frag_name = child[i]['Value']
            query += frag_name 
            query += union
        
        i = len(child)-1
        frag_name = child[i]['Value']
        query += frag_name
        cond = len(frag_cond)
        query += ');'
        
        temp_tables.append(nodes[n]['Value'])
        queries.append(query)
        return nodes[n]
    
    elif opt == 'Join':
        child = []
        vis[n] = 1
        for i in range(num+1):
            if vis[i] == 0 and adj[n][i] == 1:
                child.append(dfs(i))
        
        condition = nodes[n]['Condition']
        cond = (condition.strip()).split('=')
        left = (cond[0].strip()).split('.')
        right = (cond[1].strip()).split('.')
        query = ''
        
        if left[1] == right[1]:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS (Select * From '
            query += child[0]['Value'] + ' Inner Join ' + child[1]['Value'] + ' using('
            query += right[1]
        else:
            query = 'Create Table ' + nodes[n]['Value'] + ' AS ('
            select = 'Select * From '
            where = ' Where '
            query += select + child[0]['Value'] + ',' + child[1]['Value']
            query += where
            query += nodes[n]['Condition']
        query += ');'
        temp_tables.append(nodes[n]['Value'])
        queries.append(query)
        return nodes[n]


def final_query():
    i = num
    query = ''
    query1 = ''
    query2 = ''
    query3 = ''
    while i > 0 and nodes[i]['Key'] != 'Select':
        opt = nodes[i]['Key']
        vis[i] = 1
        if  opt == 'Project':
            query = 'Select '
            col = nodes[i]['Condition']
            for j in range(len(col)):
                query += col[j]
                query += ','
            query -= ','
            query += ' from '
        
        elif opt == 'GROUP BY':
            query2 = ' ' + opt + ' '
            col = nodes[i]['Condition']
            for j in range(len(col)):
                query2 += col[j]
                query2 += ','
            query2 -= ','
        
        elif opt == 'HAVING':
            query3 = ' ' + opt + ' '
            cond = nodes[i]['Condition']
            for j in range(len(cond)-1):
                query3 += cond[j]
                query3 += ' and '
            query3 += cond[len(cond)-1]
        i -= 1

    
    if nodes[i]['Key'] == ' Select ':
        vis[i] = 1
        temp = dfs(i)
        query1 = temp['Value']
        cond = nodes[i]['Condition']
        if len(cond) > 0:
            query1 += ' where '
            for j in range(len(cond)-1):
                query1 += cond[j]
                query1 += ' and '
            query1 += cond[len(cond)-1]
    
    query += query1 + query2 + query3 + ';'
    queries.append(query)
    return query