import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import os 

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsDatanodeClient, MASTER_DATABASE_HOST, MASTER_DATABASE_USER, MASTER_DATABASE_PASSWORD, MASTER_DATABASE_DATABASE
from Util import getLogger,getMyIP



def communicate(port,qSQLs):
    getLogger().info("Port {} is lisening for clients to add new users".format(port))
    mydb = mysql.connector.connect(
        host= MASTER_DATABASE_HOST,
        user= MASTER_DATABASE_USER,
        passwd= MASTER_DATABASE_PASSWORD,
        database= MASTER_DATABASE_DATABASE,
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (getMyIP(),port))

    while True:
        userName,Email,Password = socket.recv_string().split(' ')
        try:
            # getLogger().info("Port {}, client need to signup, username={} email={}".format(port,userName,Email))
            insertMasterSql="INSERT INTO Users (UserName, Email, Pass) VALUES ('{}','{}',SHA('{}'));".format(userName,Email,Password)
            dbcursour.execute(insertMasterSql)
            retriveSql="SELECT UserID from Users where UserName='{}' and Email='{}' and Pass=SHA('{}')".format(userName,Email,Password)
            dbcursour.execute(retriveSql)            
            userID = dbcursour.fetchall()[0][0]
            insertSlaveSql="INSERT INTO Users (UserID, UserName, Email, Pass) VALUES ('{}','{}','{}',SHA('{}'));".format(userID,userName,Email,Password)
            qSQLs.put(insertSlaveSql)
            socket.send_string("{}".format(userID))
            # getLogger().info("inserted into user databse, and send to slaves UserID={}, UserName={}, Email={}".format(userID,userName,Email))
        except Exception as e:
            # getLogger().info("Port {}, client need to signup but can't signup, username={} email={} password={} error is {}".format(port,userName,Email,Password,e.message))
            socket.send_string("-2")

if __name__ == '__main__':
    pass
    # portsMasterClient = ["5556"]

    # mydb = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     passwd="",
    #     database="lookUpData"
    # )

    # dbcursour = mydb.cursor()

    # dbcursour.execute("select IP from machines")
    # IPsRow = dbcursour.fetchall()
    # IPsM =  [str(IP[0]) for IP in IPsRow]

    # portsAvailable = mp.Manager().dict()
    # for IP in IPsM:
    #     portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    # ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    # for p in ps: p.start()
    # for p in ps: p.join()

