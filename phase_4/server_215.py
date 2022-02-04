# python3 -m Pyro4.naming
# Main site
import Pyro4
# import sqlparse
import mysql.connector
from mysql.connector.constants import ServerFlag
import logging


@Pyro4.expose
class HomeDatabase():

    def __init__(self):
        self.home = mysql.connector.connect(
            user="Maxslide", password="iiit123", host="localhost", database="QuarantinedAgain")
        self.cursor = self.home.cursor()
        logging.basicConfig(level=logging.INFO, filename='Participant_215.log')

    def two_phase_message(self,message,query):
        ready_state = 1
        print(message,query)
        if(message == "prepare"):
            logging.info("Recieved From Clinet " + message + " ")
            if ready_state == 0:
                print("abort")
                logging.info("sending vote-abort")
                return "vote-abort"
            else :
                print("ready")
                try :
                    for i in query:
                        self.cursor.execute(i)
                    logging.info("vote-commit")
                    return "vote-commit"
                except :
                    logging.info("vote-abort")
                    return "vote-abbort"
        elif(message == "COMMIT"):
            logging.info("message COMMIT recieved From Client")
            self.home.commit()
            logging.info("Commited changes")
            print("COMMITED")
            return ("Site 215 commited successfully")
        else:
            logging.info("message to ABORT")
            self.home.rollback()
            logging.info("ABORTING Changes")
            print("ABORTED")
            return ("Site 215 Aborted")


    def check_connection(self):
        print("215 connection")
        return "Connected successfully 215"


obj = HomeDatabase()
print(obj.check_connection(), "self")
Pyro4.Daemon.serveSimple(
    {obj: 'Graph'}, host='10.3.5.215', port=9090, ns=False)
# Pyro4.Daemon.serveSimple({obj : 'Graph'},host='127.0.0.1', port=9090, ns=False)
