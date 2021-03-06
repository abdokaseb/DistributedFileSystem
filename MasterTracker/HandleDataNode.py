import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector
import copy
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Constants import portsDatanodeClient, MASTER_TRAKER_HOST, MASTER_TRAKER_USER, MASTER_TRAKER_PASSWORD, MASTER_TRAKER_DATABASE
from Util import getMyIP, getLogger


machineIP = getMyIP()


def success(portsAvailable,rootIP, port):
    mydb = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    dbcursour = mydb.cursor()
    dbcursour.execute("select id from machines")
    rows = dbcursour.fetchall()
    IDs =  [str(row[0]) for row in rows]


    # Socket to talk to server
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind ("tcp://%s:%s" % (rootIP, port))
    [socket.setsockopt_string(zmq.SUBSCRIBE, ID) for ID in IDs]

    SQL = "SELECT INET_NTOA(IP) FROM machines WHERE ID = %s"
    while True:
        ID, recvPort = socket.recv_string().split()

        dbcursour.execute(SQL,(ID,))
        rows = dbcursour.fetchall()

        a = portsAvailable[rows[0][0]] 
        if recvPort not in a:
            a.append(recvPort)
            print("Port {} back from Machine ID={}, IP={} ".format(recvPort,ID,rows[0][0]))
        portsAvailable[rows[0][0]] = a

def uploadSucess (rootIP, port):
    getLogger().info("uploadSucess IP:Port {}:{} started".format(rootIP,port))
    mydb = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    dbcursour = mydb.cursor()

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind ("tcp://%s:%s" % (rootIP, port))
    SQL = "INSERT INTO files (UserID, machID, fileName) VALUES (%s,%s,%s)"
    while True:
        UserID, mcahID, fileName = socket.recv_string().split()
        getLogger().info("Uploded sucessfully: UserID={}, machID={}, fileName={} ".format(UserID, mcahID, fileName))
        dbcursour.execute(SQL,(UserID, mcahID, fileName))
        getLogger().info("Uploded sucessfully Added to database: UserID={}, machID={}, fileName={} ".format(UserID, mcahID, fileName))
        socket.send_string("1")

if __name__ == "__main__": 
    successPort = "7001"
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select id,INET_NTOA(IP) from machines")
    rows = dbcursour.fetchall()
    ID_IP =  {str(row[0]):str(row[1]) for row in rows}

    IPsM =  ID_IP.values()

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]


    success(ID_IP.keys(),portsAvailable,machineIP,successPort)


	
