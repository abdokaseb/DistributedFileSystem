import sys,zmq,time,mysql.connector,os
import copy,json,random ,multiprocessing as mp 
sys.path.append("./")
from Util import getMyIP
from Constants import MIN_REPLICA_COUNT,defaultAvaliableRepiclaPortsDataNodeDataNode,  MASTER_TRAKER_HOST, MASTER_TRAKER_USER, MASTER_TRAKER_PASSWORD, MASTER_TRAKER_DATABASE
# INET_NTOA IPuintToStr 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

db = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )


def fakeReplication(fakeUserID,fakeMachId,fileName):
    dbcursour = db.cursor()
    insertFakeRecodQuery = "insert into files  values ({},{},'{}')".format(fakeUserID,fakeMachId,fileName)
    dbcursour.execute(insertFakeRecodQuery)

def releasePorts(srcMachine,dstMachine,availReplicaPorts):
    a,b = availReplicaPorts[srcMachine[0]],availReplicaPorts[dstMachine[0]] 
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
    return dbcursour.fetchall()[0][0]
    

def getFilesToReplicate():
    minRepCnt = min(getMachinesCount(), MIN_REPLICA_COUNT)
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