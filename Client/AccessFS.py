import zmq
import sys
import json
import multiprocessing as mp
import os
import threading as th
import random
sys.path.insert(0,"../MasterTracker/")
from replicaUtilities import getMyIP

_FINISHTHREAD = 0

MasterMachineIP = '192.168.137.147'
portsMasterClient = ["5001","5002","5003","5004","5005","5006"]
clientDownloadPorts = ["8001", "8002", "8003", "8004","8005", "8006"]
portsHandleClentsToSlaves = ["8201","8202","8203","8204","8205","8206"] 
portsdatabaseClients=["7001","7002","7003","7004","7005","7006"]
DIR = "E:\\Alb 3ammar"
clientUploadIpPort= (getMyIP(),"7005")
 
def userInput(socket):
    UserID=-2;
    ErrorMessage=""
    ContinueCheck=""
    Function=""
    print("Welcome, Please press 1 to sign in or 2 to sign up with a new account")
    while(UserID ==-2 and ContinueCheck=="2"):    
        DatbaseInput=input()
        if(DatbaseInput=="1"):
            print("Please Enter Your User Name")
            UserName= input()
            print("Please Enter Your Password")
            Password= input()
            #Kasep Function Call to check the User Data Using Slaves and ID Update
            UserID=requestDatabaseSlave(MasterMachineIP,portsHandleClentsToSlaves,UserName,Password)
            if(UserID== -2):
                #print(ErrorMessage)
                print("Press 1 to End process or 2 to enter again")
                ContinueCheck=input()
        elif (DatbaseInput=="2"):
            print("Please Enter an E-mail Address")
            EmailAddress= input()
            print("Please Enter your Name")
            userName= input()
            print("Please Enter your Password")
            Password= input()
            #Kaseb Function Call to insert user in the database and ID Update
            UserID=SignUp(MasterMachineIP,portsMasterClient,userName,EmailAddress,Password) #Needs Check
            if(UserID==-2):
                print(ErrorMessage)
                print("Press 1 to End process or 2 to enter again")
                ContinueCheck=input()
    if(ContinueCheck=="1"):
        sys.out()
    print("Please, Press No. of function you want:")
    print("Press 1 to Show your files")
    print("Press 2 to Upload file")
    print("Press 3 to Download file")
    Function = input()
    check="1"
    functionCheck=""
    FileName=""
    while(check=="1"):    
        if(Function =="1"):
            #Fuction Call for LS
            LSAction("LS",socket,UserID)
            if(functionCheck=="-1"):
                print(ErrorMessage)
            print("Please Press 1 to Use another function or 2 to End")
            check=input()
        elif(Function =="2"):
            #Fuction Call for Download
            print("Please, Enter the fileName to be downloaded")
            FileName=input()
            DownloadAction("DOWNLOAD",socket,FileName,UserID)
            if(functionCheck=="-1"):
                print(ErrorMessage)
            print("Please Press 1 to Use another function or 2 to End")
            check=input()
            
        elif (Function== "3"):
            print("Please, Enter the file to be Uploaded")
            FileName=input()
            UploadAction("UPLOAD",socket,FileName,UserID)
            if(functionCheck=="-1"):
                print(ErrorMessage)
            print("Please Press 1 to Use another function or 2 to End")
            check=input()
        else: 
            print("Please Press 1 to Use another function or 2 to End")
            check=input()
def requestDatabaseSlave(IP,ports,userName,password):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    [socket.connect("tcp://%s:%s" % (IP,port)) for port in ports]
    socket.send_string("need")
    msg = socket.recv_string() 
    socket.close()

    retriveSql = "SELECT UserID FROM Users WHERE UserName='{}' and Pass ='{}' ;".format(userName,password)

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://%s" % (msg))
    socket.send_string(retriveSql)
    userID = socket.recv_string() 
    socket.close()
    
    return userID
def Upload(ipPort, fileName):
    pushContext = zmq.Context()
    pushSocket = pushContext.socket(zmq.PUSH)
    pushSocket.hwm = 10
    # change this ip (client Device) with something general
    pushSocket.bind("tcp://%s:%s"%(ipPort))
    ############################
    with open(DIR+fileName, "rb") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:                                                     # push - pull to send the video
            pushSocket.send(chunk)
            #print ('count')
            chunk = f.read(CHUNK_SIZE)
    
    f.close()
    pushSocket.send(b'')                       ## send EOF to end push pull comminucation
    pushSocket.close()
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
    numberOfPorts = [len(DataNodePorts) for i in range(len(DataNodePorts))]

    
    parameters = map(lambda a, b, c, d, e, f, g: (a, b, c, d, e, f, g),
                     ports, userActions, userIDs, fileNames, parts, chunkSizes, numberOfPorts)
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
def DownloadAction(userAction,socket,fileName,userID):
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],fileName))

    message = socket.recv_json()  #this message is the array of [IP:port] of size 6 for the mechine (e.g [192.168.1.9:5554,.....,])
        # communicate with the data node to download
    print(message)
    message = json.loads(message)

    Download(message)
        
        #print(message)
        #if message == "ERROR 404":
        #    print("Sorry We Are Very Busy")
        #pass
def LSAction(userAction,socket,userID):
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
    message = socket.recv_json()
    files = json.loads(message)
    print("the list of files are:")
    for i,file in enumerate(files,start=1): print("\t{}- {}".format(i,file))
def UploadAction(userAction,socket,fileName,userID):
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
    socket.send_string(clientUploadIpPort[0]+":"+clientUploadIpPort[1])

    message = socket.recv_string()
        # response from the datanode that the needed type of operation have been send

    Upload(clientUploadIpPort, fileName)


     ######

        #print(message)
        #if message == "ERROR 404":
        #    print("Sorry We Are Very Busy")
        #pass
def downloadPart(port,userAction,userId,fileName,partNum,chunkSize,numberOfPorts):
    
    
    context = zmq.Context()
    opSocket = context.socket(zmq.REQ)
    opSocket.connect('tcp://'+port)
    opSocket.send_string("{} {} {} {} {} {}".format(
        userAction, userId, fileName, partNum, chunkSize, numberOfPorts))
    print("type of operation and user and file and chunkazes name have been send")
    message = opSocket.recv_string()
    print(message)

    opSocket.send_string("172.28.178.37:"+str(clientDownloadPorts[partNum]))
    print ("send ip push pull to datanode port")
    message = opSocket.recv_string()
    print(message)

    pullSocket = context.socket(zmq.PULL)
    pullSocket.hwm = 10
    pullSocket.connect("tcp://"+"172.28.178.37:" +
                       str(clientDownloadPorts[partNum]))

    fileobj = open(DIR+str(userId)+"_"+str(partNum)+"_"+fileName, 'wb+')
    while True:
        chunk = pullSocket.recv()
        #print('data received in '+str(partNum))
        #counter = counter+1
        if chunk is b'':
            print('condition satisfied')
            break
        fileobj.write(chunk)

    fileobj.close()
def SignUp(IP,Port,userName,Email,Password):
    pass


if __name__ == "__main__":

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    for port in portsMasterClient: socket.connect ("tcp://%s:%s" % (portsMasterClient,port))

    userInput(socket)
    socket.close()  
#(MasterTrakerIP,portsHandleClentsToSlaves,"tye","try")
#portsdatabaseClients = ["7001","7002","7003","7004","7005","7006"]
#context = zmq.Context()
#socket = context.socket(zmq.REQ)
#[socket.connect("tcp://%s:%s" % (MasterTrakerIP,port)) for port in portsdatabaseClients]
#socket.send_string("INSERT INTO Users (UserID, UserName, Email, Pass) VA+LUES (25,'touny','abdo', 'abdo');")
#requestDatabaseSlave(MasterTrakerIP,portsHandleClentsToSlaves,"tye","try")
#USERACTIONS = {'UPLOAD':0,'DOWNLOAD':1,'LS':2}
