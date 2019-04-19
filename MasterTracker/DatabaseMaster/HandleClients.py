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




def communicate(port,qSQLs):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData",
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % port)

    while True:
        userName,Email,Password = socket.recv_string().split(' ')
        try:
            retriveSql="INSERT INTO Users (UserName, Email, Pass) VALUES ({},{},{});".format(userName,Email,Password)
            dbcursour.execute(retriveSql)
            qSQLs.put(retriveSql)
            retriveSql="Select UserID from Users where UserName={} and Email={} and Pass={}".format(userName,Email,Password)
            dbcursour.execute(retriveSql)            
            socket.send_string("{}".format(dbcursour.fetchone()[0]))
        except:
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

