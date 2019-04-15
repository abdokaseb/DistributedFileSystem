from replicaUtilities import *

def sendJsonTimOut(socket,obj,timeNS):
    poller = zmq.Poller()
    poller.register(socket,zmq.POLLOUT)
    socket.send_json(obj)
    res = poller.poll(timeNS)
    if(len(res)==0):
        raise(Exception("can't notify datanode cos no reply from data node"))


def notifyMachinesAndConfirmReplication(srcMach,dstMach,fileName,availReplicaPorts,fakeUserId,fakeMachId): 
    context = zmq.Context()
    socketSrc,socketDst = context.socket(zmq.REQ),context.socket(zmq.REQ)
    confirmSocket = context.socket(zmq.REP)
    availPort = confirmSocket.bind_to_random_port("tcp://*",min_port = 1500,max_port = 4000,max_tries = 50)
    try:
        socketSrc.connect("tcp://%s:%s" % (srcMach[1],srcMach[2]))
        sendJsonTimOut(socketSrc,{"sendToIpPort":dstMach,"fileName":fileName,"confirmSuccesOnAddress":(getMyIP(),availPort),"src":True},1000)

        socketDst.connect("tcp://%s:%s" % (dstMach[1],dstMach[2]))
        sendJsonTimOut(socketDst,{"recvFromIpPort":dstMach,"fileName":fileName,"userID":srcMach[-1],"src":False},1000)

        success = confirmSocket.recv()
        if(success == "success"):
            confirmReplication(fakeUserId,fakeMachId,srcMach[-1],dstMach[0],fileName)
            print("Replication done successfully  from machine {} to machine {}".format())
        else:
            removeReplication(fakeUserId,fakeMachId,fileName)
    except Exception as e:
        removeReplication(fakeUserId,fakeMachId,fileName)
        print("something went wrong on notifying machine " + str(e))

    releasePorts(srcMach,dstMach,availReplicaPorts)


def getSrcDstMach(fileName,availReplicaPorts):
    dbcursour = db.cursor()
    srcMachQuery = "select ID,INET_NTOA(IP),UserID from machines,files where isAlive = 1 and machID = ID and fileName ='{}'".format(fileName)
    dstMachQuery = "select ID,INET_NTOA(IP) from machines,files where isAlive = 1 and machID = ID and fileName !='{}'".format(fileName)
    dbcursour.execute(srcMachQuery)
    srcMachines =  dbcursour.fetchall() 
    dbcursour.execute(dstMachQuery)
    dstMachines =  dbcursour.fetchall() 

    srcMachine = dstMachine = None # the chosen ones
    for machId,machIP,userID in srcMachines:
        if(len(availReplicaPorts[machId])>0):
            srcMachine = machId ,machIP, availReplicaPorts[machId][0],userID
        else:
            return None
        
    for machId,machIP in dstMachines:
        if(len(availReplicaPorts[machId])>0):
            dstMachine = machId, machIP, availReplicaPorts[machId][0]
            availReplicaPorts[machId].pop(0)
            availReplicaPorts[srcMachine[0]].pop(0)
            return srcMachine,dstMachine
        else:
            return None
        

def replicate(availReplicaPorts):

    while True:
        filesToReplicate = getFilesToReplicate()
        print("Files to replicate",filesToReplicate)
        for fileName,_ in filesToReplicate:
            try:
                srcMach,dstMach = getSrcDstMach(fileName,availReplicaPorts)
                print("file, src_machine ,dst_machine: {} , {}, {}".format(fileName,srcMach,dstMach))
            except Exception as e:
                print(e)
                print("can't find avaliabe src or distnation machine for file "+fileName)
            else:
                fakeUserId, fakeMachId = random.randint(-1000000,-1),random.randint(-1000000,-1) 
                try:
                    fakeReplication(fakeUserId,fakeMachId,fileName) # if uploading process take so long may be try to upload same file issued having more unccessary replication
                    prc = mp.Process(target = notifyMachinesAndConfirmReplication,args=(srcMach,dstMach,fileName,availReplicaPorts,fakeUserId,fakeMachId)).start()
                except Exception as e:
                    removeReplication(fakeUserId,fakeMachId,fileName)
                    print("something went wrong on no creating notifying machine proccess or inserting fake repliction in data base" + str(e))
            time.sleep(3)
        time.sleep(6)
        print(availReplicaPorts)


if __name__ == "__main__": 
    machines_files = [(8,'gello'),(3,'gello'),(2,'cello'),(1,'hello'),(2,'hello'),(10,'cello')]
    ip = getMyIP();
    rq = "delete from files"
    q = "insert into files values(10,{},'{}')"
    qa = "update machines set isAlive = 1,IP = INET_ATON('{}') where ID = {}"
    dbcursor = db.cursor()
    dbcursor.execute(rq)
    db.commit()
    for m,f in machines_files:
        dbcursor.execute(q.format(m,f))
        if(random.randint(0,9)%2==0):
            dbcursor.execute(qa.format(ip,m))
        db.commit()
    
    availReplicaPorts = mp.Manager().dict({1:['1111','1112','1113'],2:['2221','2222','2223'],3:['3331','3332','3333'],8:['8881','8882','8883'],10:['9991','9992','9993']})
    replicate(availReplicaPorts)

	