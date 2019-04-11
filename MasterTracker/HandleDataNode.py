import sys
import zmq
import time
import multiprocessing as mp 
import mysql.connector
import copy


def getMachineIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
machineIP = getMachineIP()


def success(portsAvailable,rootIP, port):
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
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

    SQL = "SELECT IP FROM machines WHERE ID = %s"
    while True:
        ID, recvPort = socket.recv_string().split()

        dbcursour.execute(SQL,(ID,))
        rows = dbcursour.fetchall()

        a = portsAvailable[rows[0][0]] 
        if recvPort not in a:
            a.append(recvPort)
            print("Port {} back from Machine ID={}, IP={} ".format(recvPort,ID,rows[0][0]))
        portsAvailable[rows[0][0]] = a


if __name__ == "__main__": 
    successPort = "7001"
    portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select id,IP from machines")
    rows = dbcursour.fetchall()
    ID_IP =  {str(row[0]):str(row[1]) for row in rows}

    IPsM =  ID_IP.values()

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]


    success(ID_IP.keys(),portsAvailable,machineIP,successPort)


	