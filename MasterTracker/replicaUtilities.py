import sys,zmq,time,mysql.connector
import multiprocessing as mp ,copy,json,random ,logging

# INET_NTOA IPuintToStr 
logging.basicConfig(filename='../logs/replicaLog.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')

minReplicasCount = 5

db = mysql.connector.connect(
            host="localhost",
            user="root",
            passwd="",
            database="lookUpData"
        )


def fakeReplication(fakeUserID,fakeMachId,fileName):
    dbcursour = db.cursor()
    insertFakeRecodQuery = "insert into files  values ({},{},'{}')".format(fakeUserID,fakeMachId,fileName)
    dbcursour.execute(insertFakeRecodQuery)
    db.commit()

def releasePorts(srcMachine,dstMachine,availReplicaPorts):
    availReplicaPorts[srcMachine[0]].append(srcMachine[2])
    availReplicaPorts[dstMachine[0]].append(dstMachine[2])


def confirmReplication(fakeUserID,fakeMachId,realUserID,realMachId,fileName):
    dbcursour = db.cursor()
    insertFakeRecodQuery = "update files set UserID={}, machID={} where fileName='{}' and UserID={} and machID={}"
    insertFakeRecodQuery = insertFakeRecodQuery.format(realUserID,realMachId,fileName,fakeUserID,fakeMachId)
    dbcursour.execute(insertFakeRecodQuery)
    db.commit()

def removeReplication(fakeUserID,fakeMachId,fileName):
    dbcursour = db.cursor()
    removeFakeRecodQuery = "delete from files where fileName='{}' and UserID={} and machID={}"
    removeFakeRecodQuery = removeFakeRecodQuery.format(fileName,fakeUserID,fakeMachId)
    dbcursour.execute(removeFakeRecodQuery)
    db.commit()


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


def getMyIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
