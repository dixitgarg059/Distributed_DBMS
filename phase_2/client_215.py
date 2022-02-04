from os import curdir
import Pyro4
from mysql.connector import cursor
import sqlparse
import mysql.connector
from mysql.connector.constants import ServerFlag
import base64



@Pyro4.expose
class Client():

    def __init__(self, link1, link2,link3):
        self.site_213_link = link1
        self.site_214_link = link2
        self.site_215_link = link3

        self.site_213 = Pyro4.Proxy(link1)
        self.site_214 = Pyro4.Proxy(link2)
        self.site_215 = Pyro4.Proxy(link3)
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
        self.cursor = self.home.cursor()

    def execute_site_214(self,query):
        self.site_214.execute_query(query)

    def execute_site_213(self,query):
        self.site_213.execute_query(query)
    
    def execute_site_213(self,query):
        self.site_213.execute_query(query)

    def Two_Phase_Commit(self,query):
        # We need to send query to each site to update based on allocation schema
        Table_Name = "Table_Name"
        q = "select Table_Name, Frag_Name, Site_Id from Tables as T, Frag_Table as F, Allocation as A Where T.Table_Id = F.Table_Id and F.Frag_Id = A.Frag_Id  and Table_Name = " +Table_Name+";"
        self.cursor.execute(q)
        print('begin_commit')
        for i in self.cursor:
            #table_name = i[0]
            #Frag_Name = i[1]
            #Site_Id = i[2]

            pass
        return

        

link1 = "PYRO:Graph@10.3.5.213:9090"
link2 = "PYRO:Graph@10.3.5.214:9090"
link3 = "PYRO:Graph@10.3.5.215:9090"
obj_1 = Client(link1,link2,link3)
print(obj_1.site_213.check_connection())
print(obj_1.site_214.check_connection())
print(obj_1.site_215.check_connection())

obj_1.site_214.Send_Create_Table("TestingTable",obj_1.site_213_link)
obj_1.site_214.Send_Create_Table("TestingTable",obj_1.site_215_link)

# CREATE TABLE TestingTable(A int, B int);
# Insert Into TestingTable Values (1,2);
# Insert Into TestingTable Values (2,3);
# Insert Into TestingTable Values (3,4);
# Insert Into TestingTable Values (4,5);
# Insert Into TestingTable Values (5,6);
