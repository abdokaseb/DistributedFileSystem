import sys,zmq,time,mysql.connector,os
import copy,json,random ,multiprocessing as mp 
sys.path.append("./")
from Util import getMyIP
from Constants import MIN_REPLICA_COUNT,defaultAvaliableRepiclaPortsDataNodeDataNode,  MASTER_TRAKER_HOST, MASTER_TRAKER_USER, MASTER_TRAKER_PASSWORD, MASTER_TRAKER_DATABASE, MAX_NUMBER_MACHINES
# INET_NTOA IPuintToStr 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



def fakeReplication(fakeUserID,realMachId,fileName):
    db = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    dbcursour = db.cursor()
    insertFakeRecodQuery = "insert into files  values ({},{},'{}')".format(fakeUserID,realMachId,fileName)
    dbcursour.execute(insertFakeRecodQuery)
    print("insrted into database ... records {}".format(dbcursour.rowcount))

def releasePorts(srcMachine,dstMachine,availReplicaPorts):
    a,b = availReplicaPorts[srcMachine[0]],availReplicaPorts[dstMachine[0]] 
    a.append(srcMachine[2]) ; b.append(dstMachine[2])
    availReplicaPorts[srcMachine[0]], availReplicaPorts[dstMachine[0]]= a,b


def removeReplication(fakeUserID,realMachId,fileName):
    db = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    dbcursour = db.cursor()
    removeFakeRecodQuery = "delete from files where fileName='{}' and UserID={} and machID={}"
    removeFakeRecodQuery = removeFakeRecodQuery.format(fileName,fakeUserID,realMachId)
    dbcursour.execute(removeFakeRecodQuery)


def getMachinesCount():
    return MAX_NUMBER_MACHINES
    

def getFilesToReplicate():
    db = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    minRepCnt = min(getMachinesCount(), MIN_REPLICA_COUNT)
    dbcursour = db.cursor()
    countFilesReplicasQuery = "select fileName,count(*) as 'repCnt' from files group by(fileName) having repCnt < {}".format(minRepCnt)
    dbcursour.execute(countFilesReplicasQuery)
    return dbcursour.fetchall()

def fillAvailReplicaPorts(availReplicaPorts):
    existedIds = availReplicaPorts.keys()
    for id in range(1,MAX_NUMBER_MACHINES+1):
        if(id not in existedIds):
            availReplicaPorts[id] = defaultAvaliableRepiclaPortsDataNodeDataNode