# python3 -m Pyro4.naming
# Main site
import Pyro4
# import sqlparse
import mysql.connector
from mysql.connector.constants import ServerFlag


@Pyro4.expose
class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
        self.cursor = self.home.cursor()

    def Create_temp_table(self, table_name, columns):
        # Need to decide how to name the joins etc
        # complete the syntax for create table
        print("In create temp 214")
        creat_table = "CREATE TABLE "+table_name+" ( "
        for i in columns[:-1]:
            creat_table += i + " ,"
        creat_table += columns[-1] + " );"
        print(creat_table)
        self.cursor.execute(creat_table)
        self.home.commit()
        # At this point we have a temporary table created
        return

    def insert_to_table(self, table_name, values):

        print("In insert to table 214")
        insert = "INSERT INTO " + table_name + " VALUES "
        for i in values[:-1]:
            if(len(i)) == 1:
                insert += '('+str(i[0])+')'+ ", "
            else:
                insert += str(i) + ", "
        if(len(values[-1]) == 1):
            insert += '(' + str(values[-1][0]) + ');'
        else:
            insert += str(values[-1]) + " ;"
        print(insert)
        self.cursor.execute(insert)
        self.home.commit()


    def Send_Create_Table(self, Table_Name, link):
        casete = Pyro4.Proxy(link)
        self.cursor.execute("Describe " + Table_Name + ";")
        columns = []
        for i in self.cursor:
            col = ""
            for j in range(len(i)):
                if(j == 1):
                    # print(str(i[j]))
                    col += " " + str(i[j]).strip('b').strip("'").strip('"')
                    break
                col += i[j].strip('"')
            columns.append(col)
        print(columns)
        casete.Create_temp_table(Table_Name, columns)
        self.cursor.execute("Select * From "+Table_Name+" ;")
        check = 0
        values = []
        for i in self.cursor:
            values.append(i)
            check += 1
            if check == 100:
                casete.insert_to_table(Table_Name, values)
                check = 0
                values = []
        if len(values) > 0:
            casete.insert_to_table(Table_Name, values)
            check = 0
            values = []
        self.home.commit()
        print("Done sending 214")

    def execute_query(self, query):
        self.cursor.execute(query)
        self.home.commit()
        # print(self.cursor)
        # output_list = []
        # for i in self.cursor:
        #     output_list.append(i)
        # return output_list
        return
    
    def execute_query_output(self,query):
        self.cursor.execute(query)
        # print(self.cursor)
        output_list = []
        for i in self.cursor:
            output_list.append(i)
        return output_list

    def two_phase_message(self,message):
        ready_state = 0 
        if(message == "prepare"):
            if ready_state == 0:
                print("abort")
                return "vote-abort"
            else :
                print("ready")
                return "vote-commit"
        if(message == "COMMIT"):
            pass
        if(message == "ABORT"):
            pass
        

    def check_connection(self):
        print("214 connection")
        return "Connected successfully 214"


obj = HomeDatabase()
print(obj.check_connection(), "self")
Pyro4.Daemon.serveSimple(
    {obj: 'Graph'}, host='10.3.5.214', port=9090, ns=False)
# Pyro4.Daemon.serveSimple({obj : 'Graph'},host='127.0.0.1', port=9090, ns=False)
