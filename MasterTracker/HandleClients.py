import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json


USERACTIONS = {'UPLOAD':0,'DOWNLOAD':1,'LS':2}

def communicate(portsAvailable,port):
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
        #  Wait for next request from client
        message = socket.recv_string().split()
        if int(message[1]) == USERACTIONS['LS']:
            result = listFiles(message[0],dbcursour)
            socket.send_json(result)
        elif int(message[1]) == USERACTIONS['UPLOAD']:
            result = uploadFile(portsAvailable)
            socket.send_string(result)
        elif int(message[1]) == USERACTIONS['DOWNLOAD']:
            result = downloadFile(message[0],message[2],dbcursour,portsAvailable)
            socket.send_json(result)
        

def listFiles(userID,dbcursour):
    SQL = "SELECT DISTINCT fileName FROM files WHERE userID = " + userID
    dbcursour.execute(SQL)
    rows = dbcursour.fetchall()
    result = [row[0] for row in rows]
    return json.dumps(result)

def uploadFile(portsAvailable):
    for machIP in portsAvailable.keys():
        for port in portsAvailable[machIP]:
            a = portsAvailable[machIP]
            a.remove(port)
            portsAvailable[machIP] = a
            return '{}:{}'.format(machIP,port)
    print("Sorry We Are Very Busy")
    return "ERROR 404"

def downloadFile(userID,filename,dbcursour,portsAvailable):
    SQL = "SELECT IP FROM machines WHERE ID IN (SELECT machID FROM files WHERE userID = %s and fileName = %s)"
    dbcursour.execute(SQL,(userID,filename))
    machIPsRows = dbcursour.fetchall()
    listConnections = []

    for machIPRow in machIPsRows:
        machIP = str(machIPRow[0])
        for port in portsAvailable[machIP]:
            a = portsAvailable[machIP]
            a.remove(port)
            portsAvailable[machIP] = a
            listConnections.append('{}:{}'.format(machIP,port))
            if len(listConnections) == 6:
                return json.dumps(listConnections)
            # continue or not depend mainly on is it okay to have more than one port in the same machine ?
            # continue
    print("Sorry We Are Very Busy")

    a = [connection.split() for connection in listConnections]
    for connection in listConnections:
        IP, port = connection.split(':')
        a = portsAvailable[IP] 
        a.append(port)
        portsAvailable[IP] = a
    return json.dumps("ERROR 404")

    

if __name__ == '__main__':
    portsMasterClient = ["5556"]
    portsDatanodeClient = ["6001","6002","6003","6004","6005","6006"]

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select IP from machines")
    IPsRow = dbcursour.fetchall()
    IPsM =  [str(IP[0]) for IP in IPsRow]

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    for p in ps: p.start()
    for p in ps: p.join()

