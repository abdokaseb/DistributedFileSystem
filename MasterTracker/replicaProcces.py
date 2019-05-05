from replicaUtilities import *
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Util import getMyIP

def rcvTimOut(socket,timeNS):
    poller = zmq.Poller()
    poller.register(socket,zmq.POLLIN)
    res = poller.poll(timeNS)
    if(len(res)==0):
        raise(Exception("can't notify datanode cos no reply from data node"))
    else:
        return socket.recv_string()


def notifyMachinesAndConfirmReplication(srcMach,dstMach,fileName,availReplicaPorts,fakeUserId): 
    context = zmq.Context()
    socketSrc,socketDst = context.socket(zmq.REQ),context.socket(zmq.REQ)
    confirmSocket = context.socket(zmq.REP)
    availPort = confirmSocket.bind_to_random_port("tcp://*",min_port = 1500,max_port = 4000,max_tries = 50)
    try:
        socketSrc.connect("tcp://%s:%s" % (srcMach[1],srcMach[2]))
        socketDst.connect("tcp://%s:%s" % (dstMach[1],dstMach[2]))
        socketSrc.send_string("READY")
        socketDst.send_string("READY")
        if(rcvTimOut(socketSrc,1000) == "YES" and rcvTimOut(socketDst,1000) == "YES"):
            socketSrc.send_json({"fileName":fileName, "confirmSuccesOnIpPort":(getMyIP(),availPort) ,"src":True,"userID":srcMach[-1]})
            socketSrc.recv_string();socketSrc.close()
            socketDst.send_json({"recvFromIpPort":srcMach[1:3],"fileName":fileName,"userID":srcMach[-1],"src":False})
            socketDst.recv_string();socketDst.close()
            success = confirmSocket.recv_string()
            if(success == "success"):
                confirmSocket.send_string('OK')
            removeReplication(fakeUserId,dstMach[0],fileName)
            releasePorts(srcMach,dstMach,availReplicaPorts)
    except Exception as e:
        socketDst.setsockopt(zmq.LINGER, 0)  #clear socket buffer
        socketSrc.setsockopt(zmq.LINGER, 0)
        socketDst.close()
        socketSrc.close()
        removeReplication(fakeUserId,dstMach[0],fileName)
        releasePorts(srcMach,dstMach,availReplicaPorts)
        print("ERROR: something went wrong on notifying machine "+str(e))


def getSrcDstMach(fileName,availReplicaPorts):
    db = mysql.connector.connect(
        host=MASTER_TRAKER_HOST,
        user=MASTER_TRAKER_USER,
        passwd=MASTER_TRAKER_PASSWORD,
        database=MASTER_TRAKER_DATABASE,
        autocommit = True
    )

    dbcursour = db.cursor()
    srcMachQuery = "select ID,INET_NTOA(IP),UserID from machines,files where isAlive = 1 and machID = ID and fileName ='{}' and UserID > 0".format(fileName)
    dstMachQuery = "select ID,INET_NTOA(IP) from machines where isAlive =1 and ID not in (select distinct(machID) from files where fileName ='{}')".format(fileName)
    dbcursour.execute(srcMachQuery)
    srcMachines =  dbcursour.fetchall() 

    dbcursour.execute(dstMachQuery)
    dstMachines =  dbcursour.fetchall()
    # print(srcMachines)
    # print('----------------------------------------')
    # print(dstMachines)

    #from random import choice
    srcMachine = dstMachine = None # the chosen ones
    for machId,machIP,userID in srcMachines:
        if(len(availReplicaPorts[machId])>0):
            srcMachine = machId ,machIP, availReplicaPorts[machId][0],userID
            #srcMachine = machId ,machIP, choice(availReplicaPorts[machId]),userID
            break;

    if(srcMachine == None):
        raise(Exception())

    for machId,machIP in dstMachines:
        if(len(availReplicaPorts[machId])>0 and machId != srcMachine[0]):
            dstMachine = machId, machIP, availReplicaPorts[machId][0]
            a , b = availReplicaPorts[machId],availReplicaPorts[srcMachine[0]]
            a.pop(0) ; b.pop(0)
            availReplicaPorts[machId],availReplicaPorts[srcMachine[0]] = a,b
            # dstMachine = machId, machIP, choice(availReplicaPorts[machId])
            # a , b = availReplicaPorts[machId],availReplicaPorts[srcMachine[0]]
            # a.remove(dstMachine[-1]) ; b.remove(srcMachine[-2])
            # availReplicaPorts[machId],availReplicaPorts[srcMachine[0]] = a,b
            
            return srcMachine,dstMachine
            
    if(dstMachine == None):
        raise(Exception())


def replicate(availReplicaPorts):
    while True:
        replicate.fakeUserId-=1
        if(replicate.fakeUserId < -1000000000):
            replicate.fakeUserId = -1

        fillAvailReplicaPorts(availReplicaPorts)
        filesToReplicate = getFilesToReplicate()
        if len(filesToReplicate):
            print(filesToReplicate)
            print("Files to replicate {}".format(filesToReplicate))
        for fileName,_ in filesToReplicate:
            try:
                srcMach,dstMach = getSrcDstMach(fileName,availReplicaPorts)
                print("file, src_machine ,dst_machine: {} , {}, {}".format(fileName,srcMach,dstMach))
            except Exception as e:
                print("ERROR: can't find avaliabe src or distnation machine for file "+fileName+ str(e))
            else:
                try:
                    fakeReplication(replicate.fakeUserId,dstMach[0],fileName) # if uploading process take so long may be try to upload same file issued having more unccessary replication
                    prc = mp.Process(target = notifyMachinesAndConfirmReplication,args=(srcMach,dstMach,fileName,availReplicaPorts,replicate.fakeUserId)).start()
                except Exception as e:
                    removeReplication(replicate.fakeUserId,dstMach[0],fileName)
                    print("ERROR: something went wrong on no creating notifying machine proccess or inserting fake repliction in data base " + str(e))
        time.sleep(3)

replicate.fakeUserId = -1;


if __name__ == "__main__": 
    # machines_files = [(10,8,'B.avi'),(11,3,'V.avi')]#,(3,'gello.txt'),(2,'cello.txt'),(1,'hello.txt'),(2,'hello.txt'),(10,'cello.txt')]
    # ip = getMyIP();
    # rq = "delete from files"
    # q = "insert into files values({},{},'{}')"
    # qa = "update machines set isAlive = 1,IP = INET_ATON('{}') where ID = {}"
    # dbcursor = db.cursor()
    # dbcursor.execute(rq)
    # db.commit()
    # for uid,m,f in machines_files:
    #     dbcursor.execute(q.format(uid,m,f))
    #     #if(random.randint(0,9)%2==0):
    #     dbcursor.execute(qa.format(ip,m))
    #     dbcursor.execute(qa.format(ip,8))
    #     db.commit()
    
    availReplicaPorts = mp.Manager().dict({1:['1111','1112','1113'],0:['2220','2222','2223'],3:['3331','3332','3333'],8:['8881','8882','8883'],10:['9991','9992','9993']})
    #availReplicaPorts = mp.Manager().dict()
    replicate(availReplicaPorts)

	
