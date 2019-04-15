import zmq
import sys
import json
import multiprocessing as mp
import os

userID = 1

USERACTIONS = {'UPLOAD':0,'DOWNLOAD':1,'LS':2}
MasterTrakerIP = '192.168.137.189'

portsdatabaseClients = ["7001","7002","7003","7004","7005","7006"]
context = zmq.Context()
socket = context.socket(zmq.REQ)
[socket.connect("tcp://%s:%s" % (MasterTrakerIP,port)) for port in portsdatabaseClients]

socket.send_string("INSERT INTO Users (UserID, UserName, Email, Pass) VALUES (25,'touny','abdo', 'abdo');")
message = socket.recv_string()
print(message)
sys.exit(1)


USERACTIONS = {'UPLOAD':0,'DOWNLOAD':1,'LS':2}
MasterTrakerIP = '192.168.137.147'

portsMasterClient = ["5001","5002","5003","5004","5005","5006"]

context = zmq.Context()
socket = context.socket(zmq.REQ)

CHUNK_SIZE = 500

DIR = "C:\\Users\\ramym\\Desktop\\client\\"  ######## default directory of client machine to download or upload files


for port in portsMasterClient: socket.connect ("tcp://%s:%s" % (MasterTrakerIP,port))

# GET user action, and file name for up and down
userAction = 'DOWNLOAD'
#file name shouldn't have spaces
fileName = 'Lec4.mp4'
########functions for client operations

clientDownloadPorts = ["8001", "8002", "8003", "8004","8005", "8006"]  # down load ports of data node

clientUploadPort= 7005


def Upload(port, fileName):
    pushContext = zmq.Context()
    pushSocket = pushContext.socket(zmq.PUSH)
    pushSocket.hwm = 10
    # change this ip (client Device) with something general
    pushSocket.bind("tcp://"+"192.168.137.147:"+str(port))
    ############################
    with open(DIR+fileName, "rb") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:                                                     # push - pull to send the video
            pushSocket.send(chunk)
            print ('count')
            chunk = f.read(CHUNK_SIZE)
    
    f.close()

    pushSocket.send(b'')                       ## send EOF to end push pull comminucation
    ####################################


def Download(DataNodePorts):
    ########################################
    ####### using datanode returend from the master to establish comminucation to upload the file
    print(DataNodePorts)

    CHUNK_SIZE = 500
    ports = [port for port in DataNodePorts]
    parts = [partNum for partNum in range(len(DataNodePorts))]
    userIDs = [userID for i in range(len(DataNodePorts))]
    userActions = [userAction for i in range(len(DataNodePorts))]
    chunkSizes = [CHUNK_SIZE for i in range(len(DataNodePorts))]
    fileNames = [fileName for i in range(len(DataNodePorts))]
    
    parameters = map(lambda a, b, c, d, e, f: (a, b, c, d, e, f),
                     ports, userActions, userIDs, fileNames, parts, chunkSizes)
    downloadProcesses = mp.Pool(len(DataNodePorts))
    
    downloadProcesses.starmap(downloadPart, parameters)

    downloadProcesses.close()

    downloadProcesses.join()

    with  open(DIR+fileName, 'wb+') as fileobj :
        for i in range(len(DataNodePorts)):
            with open(DIR+str(userID)+"_" +str(i)+"_"+fileName, 'rb+') as partFile:
                chunk=partFile.read(1024)
                while chunk:
                    fileobj.write(chunk)
                    chunk = partFile.read(1024)

    for i in range(len(DataNodePorts)):
        os.remove(DIR+str(userID)+"_" +str(i)+"_"+fileName)
    return 

        
        




    ############################
    ####################################

def downloadPart(port,userAction,userId,fileName,partNum,chunkSize):
    
    
    context = zmq.Context()
    opSocket = context.socket(zmq.REQ)
    opSocket.connect('tcp://'+port)
    opSocket.send_string("{} {} {} {} {}".format(
        userAction, userId, fileName, partNum, chunkSize,))
    print("type of operation and user and file and chunkazes name have been send")
    message = opSocket.recv_string()
    print(message)

    opSocket.send_string("192.168.137.147:"+str(clientDownloadPorts[partNum]))
    print ("send ip push pull to datanode port")
    message = opSocket.recv_string()
    print(message)

    pullSocket = context.socket(zmq.PULL)
    pullSocket.hwm = 10
    pullSocket.connect("tcp://"+"192.168.137.147:" +
                       str(clientDownloadPorts[partNum]))

    fileobj = open(DIR+str(userId)+"_"+str(partNum)+"_"+fileName, 'wb+')
    while True:
        chunk = pullSocket.recv()
        print('data received in '+str(partNum))
        #counter = counter+1
        if chunk is b'':
            print('condition satisfied')
            break
        fileobj.write(chunk)

    fileobj.close()


if __name__ == "__main__":

    if len(sys.argv) > 1:
        userAction = sys.argv[1]
        ## operations

    if len(sys.argv) > 2:
        fileName = sys.argv[2]
        ## fielName

    if userAction == 'LS': 
        socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
        message = socket.recv_json()
        files = json.loads(message)
        print("the list of files are:")
        for i,file in enumerate(files,start=1): print("\t{}- {}".format(i,file))
    elif userAction == 'UPLOAD':
        socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
        message = socket.recv_string()  #this message is the IP:port for the mechine (e.g 192.168.1.9:5554)
        
        DataNodePort = "tcp://"+message

        print(DataNodePort)
        socket = context.socket(zmq.REQ)  # Socket to connect with DataNode

        socket.connect(DataNodePort)

        socket.send_string("{} {} {}".format(userAction, userID, fileName))
        print("type of operation and user and file name have been send")
        message = socket.recv_string()
        # response from the datanode that the needed type of operation have been send
        print(message)

        # the new socket for uploading the data to data node to make it connect to it
        socket.send_string("192.168.137.147:"+str(clientUploadPort))

        message = socket.recv_string()
        # response from the datanode that the needed type of operation have been send

        Upload(clientUploadPort , fileName)


        ######

        #print(message)
        #if message == "ERROR 404":
        #    print("Sorry We Are Very Busy")
        #pass
    elif userAction == 'DOWNLOAD':
        socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],fileName))
        message = socket.recv_json()  #this message is the array of [IP:port] of size 6 for the mechine (e.g [192.168.1.9:5554,.....,])
        # communicate with the data node to download
        message = json.loads(message)

        Download(message)
        
        #print(message)
        #if message == "ERROR 404":
        #    print("Sorry We Are Very Busy")
        #pass
        

