import sys,zmq,time,mysql.connector
import multiprocessing as mp ,copy,json,random ,logging

# INET_NTOA IPuintToStr 

defaultAvaliableRepiclaPortsDataNodeDataNode = [str(9000+i) for i in range(20)]

minReplicasCount = 2

db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database="lookUpData",
            autocommit = True
        )


def fakeReplication(fakeUserID,fakeMachId,fileName):
    dbcursour = db.cursor()
    insertFakeRecodQuery = "insert into files  values ({},{},'{}')".format(fakeUserID,fakeMachId,fileName)
    dbcursour.execute(insertFakeRecodQuery)

def releasePorts(srcMachine,dstMachine,availReplicaPorts):
    print(availReplicaPorts[srcMachine[0]])
    a,b = availReplicaPorts[srcMachine[0]],availReplicaPorts[dstMachine[0]] 
    print(a)
    a.append(srcMachine[2]) ; b.append(dstMachine[2])
    availReplicaPorts[srcMachine[0]], availReplicaPorts[dstMachine[0]]= a,b


def confirmReplication(fakeUserID,fakeMachId,realUserID,realMachId,fileName):
    dbcursour = db.cursor()
    insertFakeRecodQuery = "update files set UserID={}, machID={} where fileName='{}' and UserID={} and machID={}"
    insertFakeRecodQuery = insertFakeRecodQuery.format(realUserID,realMachId,fileName,fakeUserID,fakeMachId)
    dbcursour.execute(insertFakeRecodQuery)

def removeReplication(fakeUserID,fakeMachId,fileName):
    dbcursour = db.cursor()
    removeFakeRecodQuery = "delete from files where fileName='{}' and UserID={} and machID={}"
    removeFakeRecodQuery = removeFakeRecodQuery.format(fileName,fakeUserID,fakeMachId)
    dbcursour.execute(removeFakeRecodQuery)


def getMachinesCount():
    dbcursour = db.cursor()
    dbcursour.execute("select count(distinct(ID)) from machines")
    return dbcursour.fetchall()
    

def getFilesToReplicate():
    minRepCnt = min(getMachinesCount(), minReplicasCount)
    dbcursour = db.cursor()
    countFilesReplicasQuery = "select fileName,count(*) as 'repCnt' from files group by(fileName) having repCnt < {}".format(minRepCnt)
    dbcursour.execute(countFilesReplicasQuery)
    return dbcursour.fetchall() 

def fillAvailReplicaPorts(availReplicaPorts):
    dbcursour = db.cursor()
    dbcursour.execute("select id from machines")
    ids = dbcursour.fetchall()
    existedIds = availReplicaPorts.keys()
    for id in ids:
        if(id[0] not in existedIds):
            availReplicaPorts[id[0]] = defaultAvaliableRepiclaPortsDataNodeDataNode
    

def getMyIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
