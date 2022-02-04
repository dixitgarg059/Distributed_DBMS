from os import curdir
import Pyro4
from mysql.connector import cursor
import sqlparse
import mysql.connector
from mysql.connector.constants import ServerFlag
import base64
import logging



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
        logging.basicConfig(level=logging.INFO, filename='Client.log')
        

    def execute_site_214(self,query):
        self.site_214.execute_query(query)

    def execute_site_213(self,query):
        self.site_213.execute_query(query)
    
    def execute_site_213(self,query):
        self.site_213.execute_query(query)

    def Two_Phase_Commit(self,query):
        # We need to send query to each site to update based on allocation schema
        # Table_Name = "Table_Name"
        # query = "UPDATE Table Where A = b"
        parsed_query = sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0].tokens
        token_list = []
        for i in parsed_query:
            if not i.is_whitespace:
                try:
                    token = []
                    for j in i.get_identifiers():
                        token.append(j)
                    token_list.append(token)
                except:
                    token_list.append(i)
        for i in range(len(token_list)):
            if(str(token_list[i]) == "UPDATE"):
                Table_Name = str(token_list[i+1])
                break
        
        
        print(token_list)
        q = "select Table_Name, Frag_Name, Site_Id from Tables as T, Frag_Table as F, Allocation as A Where T.Table_Id = F.Table_Id and F.Frag_Id = A.Frag_Id  and Table_Name = '" +Table_Name+"';"
        self.cursor.execute(q)
        print('begin_commit')
        logging.info("begin-commit")
        replies = []
        site_data = {1:[],2:[],3:[]}
        site_dict = {1 : self.site_215, 2: self.site_214, 3 : self.site_213}
        for i in self.cursor:
            table_name = i[0]
            Frag_Name = i[1]
            Site_Id = i[2]
            # temp = query.split()
            query_list = []
            flag = 0
            for i in token_list:
                if flag == 0:
                    if(str(i) == "UPDATE"):
                        flag = 1
                    query_list.append(str(i))
                    query_list.append(" ")
                else:
                    query_list.append(Frag_Name)
                    query_list.append(" ")
                    flag = 0
            final_query = ''.join(query_list)
            print(final_query)
            site_data[Site_Id].append(final_query)
            # replies.append(site_dict[Site_Id].two_phase_message("prepare", final_query))
        for Site_Id in site_data:
            print(site_data[Site_Id])
            logging.info("Sending prepare message")
            try:
                replies.append(site_dict[Site_Id].two_phase_message("prepare", site_data[Site_Id]))
            except:
                replies.append("vote-abort")

        if ("vote-abort" not in replies):
            for Site_Id in site_data:
                site_dict[Site_Id].two_phase_message("COMMIT",site_data[Site_Id])
            logging.info("Sent Message to Commit")
        else :
            logging.info("Recieved vote-abort from some site, hence Aborting")
            for Site_Id in site_data:
                try:
                    site_dict[Site_Id].two_phase_message("ABORT",site_data[Site_Id])
                except Exception as e:
                    print(e)
                    logging.info("Site is shut : " + str(Site_Id))
                    print("Site is shut : ", Site_Id)
            

        return

        

link1 = "PYRO:Graph@10.3.5.213:9090"
link2 = "PYRO:Graph@10.3.5.214:9090"
link3 = "PYRO:Graph@10.3.5.215:9090"
obj = Client(link1,link2,link3)
print(obj.site_213.check_connection())
print(obj.site_214.check_connection())
print(obj.site_215.check_connection())

# obj.site_214.Send_Create_Table("TestingTable",obj.site_213_link)
# obj.site_214.Send_Create_Table("TestingTable",obj.site_215_link)
print("Enter query : ")
query = input()
obj.Two_Phase_Commit(query)
# CREATE TABLE TestingTable(A int, B int);
# Insert Into TestingTable Values (1,2);
# Insert Into TestingTable Values (2,3);
# Insert Into TestingTable Values (3,4);
# Insert Into TestingTable Values (4,5);
# Insert Into TestingTable Values (5,6);
