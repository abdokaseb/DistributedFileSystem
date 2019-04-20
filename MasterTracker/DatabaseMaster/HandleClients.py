import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import os 

sys.path.append(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))))

from Constants import portsDatanodeClient
from Util import getLogger,getMyIP



def communicate(port,qSQLs):
    getLogger().info("Port {} is lisening for clients to add new users".format(port))
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpDataMaster",
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (getMyIP(),port))

    while True:
        userName,Email,Password = socket.recv_string().split(' ')
        try:
            getLogger().info("Port {}, client need to signup, username={} email={} password={}".format(port,userName,Email,Password))
            insertMasterSql="INSERT INTO Users (UserName, Email, Pass) VALUES ('{}','{}','{}');".format(userName,Email,Password)
            dbcursour.execute(insertMasterSql)
            retriveSql="SELECT UserID from Users where UserName='{}' and Email='{}' and Pass='{}'".format(userName,Email,Password)
            dbcursour.execute(retriveSql)            
            userID = dbcursour.fetchall()[0][0]
            insertSlaveSql="INSERT INTO Users (UserID, UserName, Email, Pass) VALUES ('{}','{}','{}','{}');".format(userID,userName,Email,Password)
            qSQLs.put(insertSlaveSql)
            socket.send_string("{}".format(userID))
            getLogger().info("inserted into user databse, and send to slaves UserID={}, UserName={}, Email={}, Pass={}".format(userID,userName,Email,Password))
        except Exception as e:
            getLogger().info("Port {}, client need to signup but can't signup, username={} email={} password={} error is {}".format(port,userName,Email,Password,e.message))
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

