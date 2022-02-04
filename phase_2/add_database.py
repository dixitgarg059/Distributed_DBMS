from logging import setLoggerClass
import mysql.connector
from mysql.connector.constants import ServerFlag
import sys
import csv
import os.path
from os import path


class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
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
            insert_into_table = "INSERT INTO " + table_name+" VALUES"
        # At this point we have a temporary table created
        return

    def Execute_Query(self, query):
        self.cursor.execute(query)
        output_list = []
        for i in self.cursor:
            output_list.append(i)
        return output_list

    def sql_queries(self, t1, t2, tab_list):

        query = "Select * From Tables"
        tables_list = self.Execute_Query(query)
        last_table_id = int(tables_list[-1][0])

        for i in tab_list:
            q = "INSERT INTO Tables VALUES (" + str(last_table_id + 1) + \
                " , '" + i[0] + "', 0, '" + i[1] + \
                "', " + str(tab_list[i]) + ");"
            print(q)
            last_table_id += 1
            for j in range(len(t1)):
                if t1[j][0] == i[0]:
                    t1[j][0] = str(last_table_id)
            self.cursor.execute(q)

        query = "Select * From Frag_Table"
        frag_list = self.Execute_Query(query)
        last_frag_id = int(frag_list[-1][0])
        for i in t1:
            q = "INSERT INTO Frag_Table VALUES (" + str(
                last_frag_id + 1) + " , '" + i[1] + "', '" + i[3] + "', " + i[0] + ");"
            last_frag_id += 1
            for j in range(len(t2)):
                if t2[j][0] == i[1]:
                    t2[j][0] = str(last_frag_id)
            self.cursor.execute(q)

        for i in t2:
            q = "INSERT INTO Allocation VALUES (" + i[0] + ", " + i[1] + ");"
            self.cursor.execute(q)

        return

    def commit(self):
        self.home.commit()
        self.home.close()


class QuarantinedAgain():

    def __init__(self, user, password, host, database, home):
        self.cnx = mysql.connector.connect(
            user=user, password=password, host=host, database=database)
        self.home = home
        self.cursor = self.cnx.cursor()
        self.temp_table_count = 0

    def Get_Fragments(self):
        query = "SELECT * FROM Frag_Table;"
        self.cursor.execute(query)
        print("id Fragment_name Condition")
        for (i, j, k, l) in self.cursor:
            print(i, j, k)

    def Get_Allocation(self, frag_id):
        query = "Select * FROM Allocation Where Frag_Id = " + \
            str(frag_id) + ";"
        self.cursor.execute(query)
        for i in self.cursor:
            # print(i)
            print("Frag_Id = " + str(i[0]) +
                  " allocated on site : " + str(i[1]))

    def Execute_Query(self, query):
        # THis function will be to get a table from the object site and saving to the current site as a temp_<that table name>
        self.cursor.execute(query)
        final_output = []
        for i in self.cursor:
            final_output.append(i)
        return final_output

    def sql_queries(self, t1, t2, tab_list):

        query = "Select * From Tables"
        tables_list = self.Execute_Query(query)
        last_table_id = int(tables_list[-1][0])

        for i in tab_list:
            q = "INSERT INTO Tables VALUES (" + str(last_table_id + 1) + \
                " , '" + i[0] + "', 0, '" + i[1] + \
                "', " + str(tab_list[i]) + ");"
            print(q)
            last_table_id += 1
            for j in range(len(t1)):
                if t1[j][0] == i[0]:
                    t1[j][0] = str(last_table_id)
            self.cursor.execute(q)

        query = "Select * From Frag_Table"
        frag_list = self.Execute_Query(query)
        last_frag_id = int(frag_list[-1][0])
        for i in t1:
            q = "INSERT INTO Frag_Table VALUES (" + str(
                last_frag_id + 1) + " , '" + i[1] + "', '" + i[3] + "', " + i[0] + ");"
            last_frag_id += 1
            for j in range(len(t2)):
                if t2[j][0] == i[1]:
                    t2[j][0] = str(last_frag_id)
            self.cursor.execute(q)

        for i in t2:
            q = "INSERT INTO Allocation VALUES (" + i[0] + ", " + i[1] + ");"
            self.cursor.execute(q)

    def commit(self):
        self.cnx.commit()
        self.cnx.close()
# Main idea to have obj of both the servers and a connection to the home server.
# We can the write to the main server the temp tables and use these to further execue the queries and display the output
# Final work flow acc to me could something like, get from obj 1, get from obj2, now perform join in out home site and then everything happens in our home site
# as soon as all this is done, we will delete all these temp tables


csv_file = str(sys.argv[1])
t1 = []
t2 = []
tab_list = {}
b = "[,]"

if path.exists(csv_file):
    with open(csv_file, 'r') as csvfile:
        csvr = csv.reader(csvfile)
        row1 = next(csvr)

        for row in csvr:
            if (str(row[0].strip()) == "" and str(row[1].strip()) == "" and str(row[2].strip()) == "" and str(row[3].strip()) == ""):
                break
            temp = []
            ta = []

            temp.append(str(row[0].strip()))
            temp.append(str(row[1].strip()))
            frag_type = str(row[2].strip())
            temp.append(frag_type + "F")

            ta.append(str(row[0].strip()))
            ta.append(frag_type + "F")

            cond = str(row[3].strip())
            for char in b:
                cond = cond.replace(char, "")
            s = ""
            f = 0
            for i in range(len(cond)):
                s += cond[i]
                if cond[i] == '=' and cond[i+1] != '"' and cond[i+1] != "'":
                    if cond[i+1].isnumeric() == False:
                        s += '"'
                        f = 1
                if f and cond[i+1] == " ":
                    s += '"'
                    f = 0
            temp.append(s)
            t1.append(temp)
            ta = tuple(ta)
            if ta not in tab_list:
                tab_list[ta] = 1
            else:
                tab_list[ta] += 1

        row1 = next(csvr)
        row1 = next(csvr)
        row1 = next(csvr)

        for row in csvr:
            temp = []
            temp.append(str(row[0].strip()))
            temp.append(str(row[1].strip()))
            t2.append(temp)

    Home = HomeDatabase()
    # obj = QuarantinedAgain("Maxslide", "iiit123",
                        #    "10.3.5.213", "QuarantinedAgain", Home)
    # obj2 = QuarantinedAgain("Maxslide", "iiit123",
                            # "10.3.5.214", "QuarantinedAgain", Home)
    Home.sql_queries(t1, t2, tab_list)
    Home.commit()
    # obj.sql_queries(t1, t2, tab_list)
    # obj.commit()
    # obj2.sql_queries(t1, t2, tab_list)
    # obj2.commit()
else:
    print("Error: File doesn't exist")
