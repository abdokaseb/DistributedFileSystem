import zmq
import time
import sys
import multiprocessing as mp
import mysql.connector
import json
import os
import random
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import math
from Constants import portsDatanodeClient, USERACTIONS, MASTER_TRAKER_HOST, MASTER_TRAKER_USER, MASTER_TRAKER_PASSWORD, MASTER_TRAKER_DATABASE, numberOfPortsToDownload
from Util import getLogger


#USERACTIONS = {'UPLOAD':0,'DOWNLOAD':1,'LS':2}

def communicate(portsAvailable,rootIP,port):
    mydb = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit=True
    )
    dbcursour = mydb.cursor()    

    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind("tcp://%s:%s" % (rootIP,port))
    getLogger().info("Start listen to clients at IP:Port {}:{}".format(rootIP,port))
    # socket.setsockopt(zmq.LINGER,0)
    # socket.close()
    # context = zmq.Context()
    # socket = context.socket(zmq.REP)
    # socket.bind("tcp://192.168.43.110:6330")
    # socket.recv_string()
    # print("HEEEEELoooooo")
    # socket.send_string("here")
    while True:
        #  Wait for next request from client
        message = socket.recv_string().split()
        print(message)
        # getLogger().info("Port {} recive request from client with id={} need instrunction {}".format(port,message[0],USERACTIONS[int(message[1])]))
        print(message)
        if int(message[1]) == USERACTIONS['LS']:
            result = listFiles(message[0],dbcursour)
            socket.send_json(result)
        elif int(message[1]) == USERACTIONS['UPLOAD']:
            result = uploadFile(message[0],message[2],dbcursour,portsAvailable)
            socket.send_string(result)
        elif int(message[1]) == USERACTIONS['DOWNLOAD']:
            result = downloadFile(message[0],message[2],dbcursour,portsAvailable)
            socket.send_json(result)
        # getLogger().info("Port {} replied to client with id={} for instunction {} with result = {}".format(port,message[0],USERACTIONS[int(message[1])],result))
        

def listFiles(userID,dbcursour):
    SQL = "SELECT DISTINCT fileName FROM files WHERE userID = " + userID
    dbcursour.execute(SQL)
    rows = dbcursour.fetchall()
    result = [row[0] for row in rows]
    return json.dumps(result)
#####################################################################################################

######### For Test the DataNode ###################### remove it
def uploadFile(userID,filename,dbcursour,portsAvailable):
    SQL = "SELECT userID FROM files WHERE userID = %s and filename = %s"
    dbcursour.execute(SQL,(userID,filename))
    _ = dbcursour.fetchall()
    if (dbcursour.rowcount != 0):
        return "You have file with the same name"

    machIPs = portsAvailable.keys()
    index = random.randint(0,len(machIPs)-1)
    machIP = machIPs[index]
    
    ports = portsAvailable[machIP]
    index = random.randint(0,len(ports)-1)
    port = ports[index]

    return '{}:{}'.format(machIP,port)

def downloadFile(userID,filename,dbcursour,portsAvailable):
    print(userID,filename)
    SQL = "SELECT INET_NTOA(IP) FROM machines WHERE ID IN (SELECT machID FROM files WHERE userID = %s and fileName = %s)"
    dbcursour.execute(SQL,(userID,filename))
    machIPsRows = dbcursour.fetchall()
    if len(machIPsRows) == 0:
        return json.dumps("NO FILE WITH THIS NAME")

    random.shuffle(machIPsRows)
    if len(machIPsRows) >  numberOfPortsToDownload:
        machIPsRows = machIPsRows[:7]
    portsPertMach = math.ceil(numberOfPortsToDownload/len(machIPsRows)) 
    connectionList = []
    for row in machIPsRows:
        machIP = row[0]
        ports = portsAvailable[machIP] 
        random.shuffle(ports)
        connectionList.extend(["{}:{}".format(machIP,ports[i]) for i in range(portsPertMach)])
    
    random.shuffle(connectionList)
    connectionList[:numberOfPortsToDownload]
    if len(connectionList)==numberOfPortsToDownload:
        return json.dumps(connectionList)
    print("Sorry We Are Very Busy")

    return json.dumps("ERROR 404")

    #return json.dumps(['172.28.178.37:6001', '172.28.178.37:6002', '172.28.178.37:6003', '172.28.178.37:6004', '172.28.178.37:6005', '172.28.178.37:6006', '172.28.178.37:6007'])
    #return json.dumps(['192.168.1.4:6001'])

    

if __name__ == '__main__':
    portsMasterClient = ["5556"]

    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="",
        database="lookUpData"
    )

    dbcursour = mydb.cursor()

    dbcursour.execute("select INET_NTOA(IP) from machines")
    IPsRow = dbcursour.fetchall()
    IPsM =  [str(IP[0]) for IP in IPsRow]

    portsAvailable = mp.Manager().dict()
    for IP in IPsM:
        portsAvailable[IP] = [port for port in portsDatanodeClient]
    
    ps = [mp.Process(target = communicate,args=(portsAvailable,port)) for port in portsMasterClient]
    for p in ps: p.start()
    for p in ps: p.join()

