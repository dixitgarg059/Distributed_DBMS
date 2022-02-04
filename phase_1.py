from logging import setLoggerClass
import mysql.connector
from mysql.connector.constants import ServerFlag


class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(user="Maxlslide",password = "iiit123",host = "localhost",database = "QuarantinedAgain")
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
        output_list = []
        for i in self.cursor:
            output_list.append(i)
        return output_list
        

class QuarantinedAgain():

    def __init__(self,user,password,host,database,home):
        self.cnx = mysql.connector.connect(user=user, password=password,host=host,database=database)
        self.home = home
        self.cursor = self.cnx.cursor()
        self.temp_table_count = 0
    def Get_Fragments(self):
        query = "SELECT * FROM Frag_Table;"
        self.cursor.execute(query)
        print("id Fragment_name Condition")
        for (i,j,k,l) in self.cursor:
            print(i,j,k)

    def Get_Allocation(self,frag_id):
        query = "Select * FROM Allocation Where Frag_Id = " + str(frag_id) + ";"
        self.cursor.execute(query)
        for i in self.cursor:
            # print(i)
            print("Frag_Id = " + str(i[0]) + " allocated on site : " +str(i[1]))

    def Execute_Query(self,query):
        # THis function will be to get a table from the object site and saving to the current site as a temp_<that table name>
        self.cursor.execute(query)
        final_output = []
        for i in self.cursor:
            final_output.append(i)
        return

# Main idea to have obj of both the servers and a connection to the home server.
# We can the write to the main server the temp tables and use these to further execue the queries and display the output
# Final work flow acc to me could something like, get from obj 1, get from obj2, now perform join in out home site and then everything happens in our home site
# as soon as all this is done, we will delete all these temp tables

Home = HomeDatabase()
obj = QuarantinedAgain("Maxslide", "iiit123","10.3.5.213","QuarantinedAgain",Home)
obj2 = QuarantinedAgain("Maxslide", "iiit123","10.3.5.214","QuarantinedAgain",Home)

