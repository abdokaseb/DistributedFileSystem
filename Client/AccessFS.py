import zmq
import sys
import json
import multiprocessing as mp
import os
import threading as th
import random
sys.path.append("../")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Constants import MASTER_FILESYSTEM_MACHINE_IP, CHUNK_SIZE, USERACTIONS, MASTER_DATABASE_MACHINE_IP, clientDownloadPorts, portsHandleClentsToSlaves, portsdatabaseClients, clientUploadIpPort, masterClientPorts
#getLogger().info("asdasdasdasd")
sys.path.insert(0,"../MasterTracker/")

from Util import getMyIP,setLoggingFile,getLogger

 
def userInput(socket):
    UserID="-2"
    ErrorMessage=""
    ContinueCheck="2"
    Function=""
    #print("Welcome, Please press 1 to sign in or 2 to sign up with a new account")
    while(UserID =="-2" and ContinueCheck=="2"):
        print("Please press 1 to sign in or 2 to sign up with a new account")    
        DatbaseInput=input()
        if(DatbaseInput=="1"):
            print("Please Enter Your User Name")
            UserName= input()
            print("Please Enter Your Password")
            Password= input()

            getLogger().info("call sign in with username {}, password {}".format(UserName,Password))
            #Kasep Function Call to check the User Data Using Slaves and ID Update
            UserID=requestDatabaseSlave(MASTER_DATABASE_MACHINE_IP,portsHandleClentsToSlaves,UserName,Password)
            getLogger().info("call sign in with username {}, password {} and get user ID equals {}".format(UserName,Password,UserID))
            if(UserID== "-2"):
                print("Worng username and password")
                print("Press 1 to End process or 2 to enter again")
                ContinueCheck=input()
        elif (DatbaseInput=="2"):
            print("Please Enter an E-mail Address")
            EmailAddress= input()
            print("Please Enter your Name")
            userName= input()
            print("Please Enter your Password")
            Password= input()

            getLogger().info("call sign up with username {}, password {}, email {}".format(userName,Password,EmailAddress))
            #Kaseb Function Call to insert user in the database and ID Update
            UserID=SignUp(MASTER_FILESYSTEM_MACHINE_IP,masterClientPorts,userName,EmailAddress,Password) 
            getLogger().info("call sign up with username {}, password {}, email {} and get user ID equals {}".format(userName,Password,EmailAddress,UserID))
            if(UserID=="-2"):
                print("Can't Sign Up, Email is already in use")
                print("Press 1 to End process or 2 to enter again")
                ContinueCheck=input()
    
    getLogger().info("user ID {}, ContinueCheck {}".format(UserID,ContinueCheck))
    if(ContinueCheck=="1"):
        sys.exit()
    check="1"
    functionCheck=""
    FileName=""
    
    DIR=""
    while(check=="1"):
        print("Please, Press No. of function you want:")
        print("Press 1 to Show your files")
        print("Press 2 to Upload file")
        print("Press 3 to Download file")
        print("Any thing else with exit")
        Function = input()    
        if(Function =="1"):
            getLogger().info("LS action need")
            #Fuction Call for LS
            LSAction("LS",socket,UserID)
            if(functionCheck=="-1"):
                print(ErrorMessage)
    
        elif(Function =="3"):
            getLogger().info("download action need")
            #Fuction Call for Download
            print("Please, Enter the fileName to be downloaded")
            FileName = input()
            print("Please, Enter the Directory to be downloaded in ")
            DIR = input()
            functionCheck=DownloadAction("DOWNLOAD",socket,DIR,FileName,UserID)
            getLogger().info("response after download {}".format(functionCheck))
            if(functionCheck=="-1"):
                print(ErrorMessage)
            # print("Please Press 1 to Use another function or 2 to End")
            # check=input()
            
        elif (Function== "2"):
            getLogger().info("upload action need")
            print("Please, Enter the file to be Uploaded")
            FileName=input()
            print("Please, Enter the DIR to be Uploaded")
            DIR=input()
            functionCheck=UploadAction("UPLOAD", socket, DIR, FileName, UserID)
            getLogger().info("response after upload {}".format(functionCheck))
            if(functionCheck==-1):
                print(ErrorMessage)
            else:
                print("Uploaded Successfully")
        else: 
            getLogger().info("wrong input")
            print("Thank you for using our drive")
            print("Secure, Fast, Safe :D")
            break


#######################################################################
######################################################################
#####################################################################
def requestDatabaseSlave(IP,ports,userName,password):
    context = zmq.Context()
   
    getLogger().info("DNS Connection started")
    socket = context.socket(zmq.REQ)
    [socket.connect("tcp://%s:%s" % (IP,port)) for port in ports]
    getLogger().info("DNS Connection finished")

    socket.send_string("need")
    msg = socket.recv_string() 
    getLogger().info("DNS replied with " + msg)

    socket.close()

    retriveSql = "SELECT UserID FROM Users WHERE UserName='{}' and Pass =SHA('{}') ;".format(userName,password)
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://%s" % (msg))
    
    socket.send_string(retriveSql)
    getLogger().info("Send request " + retriveSql)
    userID = socket.recv_string()
    getLogger().info("Receive userID {}".format(userID)) 
    socket.close()
    
    return userID

def Upload(ipPort,DIR,fileName):
    pushContext = zmq.Context()
    pushSocket = pushContext.socket(zmq.PUSH)
    pushSocket.hwm = 10
    # change this ip (client Device) with something general
    pushSocket.bind("tcp://%s:%s"%(ipPort))
    ############################
    i = 0
    with open(DIR+fileName, "rb") as f:
        chunk = f.read(CHUNK_SIZE)
        while chunk:                                                     # push - pull to send the video
            pushSocket.send(chunk)
            print (i)
            i+= 1
            chunk = f.read(CHUNK_SIZE)
    
    f.close()
    pushSocket.send(b'')                       ## send EOF to end push pull comminucation
    pushSocket.close()
    ####################################
def Download(DataNodePorts, DIR, fileName, userID, userAction):
    ########################################
    ####### using datanode returend from the master to establish comminucation to upload the file
    print(DataNodePorts)

    #CHUNK_SIZE = 500
    ports = [port for port in DataNodePorts]
    parts = [partNum for partNum in range(len(DataNodePorts))]
    userIDs = [userID for i in range(len(DataNodePorts))]
    userActions = [userAction for i in range(len(DataNodePorts))]
    chunkSizes = [CHUNK_SIZE for i in range(len(DataNodePorts))]
    fileNames = [fileName for i in range(len(DataNodePorts))]
    numberOfPorts = [len(DataNodePorts) for i in range(len(DataNodePorts))]
    directories = [DIR for i in range(len(DataNodePorts))]

    
    parameters = map(lambda a, b, c, d, e, f, g,h: (a, b, c, d, e, f, g,h),
                     ports, userActions, userIDs,directories ,fileNames, parts, chunkSizes, numberOfPorts)
    downloadProcesses = mp.Pool(len(DataNodePorts))
    
    downloadProcesses.starmap(downloadPart, parameters)
    print("after donwload ")
    downloadProcesses.close()
    print("after donwload ")
    downloadProcesses.join()
    print("after donwload ")
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
def DownloadAction(userAction,socket,DIR,fileName,userID):
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],fileName))

    message = socket.recv_json()  #this message is the array of [IP:port] of size 6 for the mechine (e.g [192.168.1.9:5554,.....,])
        # communicate with the data node to download
    message = json.loads(message)
    getLogger().info("Download ports is {}".format(message))
    if message == "ERROR 404":
        print("Sorry We Are Very Busy")
        return -1 
    elif message == "NO FILE WITH THIS NAME":
        print("You don't have files with this name")
        return -1 
    getLogger().info("Start acutal download DIR {}, fileName {}".format(DIR, fileName))
    Download(message, DIR, fileName, userID, userAction)
        
def LSAction(userAction,socket,userID):
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
    message = socket.recv_json()
    files = json.loads(message)
    getLogger().info("LS files is {}".format(files))
    print("the list of files are:")
    for i,file in enumerate(files,start=1): print("\t{}- {}".format(i,file)) 
def UploadAction(userAction, socket, DIR, fileName, userID):
    if os.path.isfile(DIR+fileName) == False:
        print("File doesn't exist")
        return -1
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],fileName))
    message = socket.recv_string()  #this message is the IP:port for the mechine (e.g 192.168.1.9:5554)
    getLogger().info("ports to upload " + message)    
    if message == "ERROR 404":
        print("Sorry We Are Very Busy")
        return -1
    if message == "You have file with the same name":
        print("You have file with the same name")
        return -1

    DataNodePort = "tcp://"+message
    # getLogger().info(DataNodePort)
            
    #print(DataNodePort)
    socket = context.socket(zmq.REQ)  # Socket to connect with DataNode
    socket.connect(DataNodePort)

    socket.send_string("{} {} {} {}".format(userAction, userID, fileName,clientUploadIpPort[0]+":"+clientUploadIpPort[1]))
    getLogger().info("type of operation and user and file name have been send")
    message = socket.recv_string()
    getLogger().info("after socket receive type of operation and filenae and user to datanode")

    # getLogger().info("messsageeeeeeeeeeee"+message)
        #response from the datanode that the needed type of operation have been send
    
        #the new socket for uploading the data to data node to make it connect to it
    # socket.send_string(clientUploadIpPort[0]+":"+clientUploadIpPort[1])

    # message = socket.recv_string()
        #response from the datanode that the needed type of operation have been send
    getLogger().info("Start acutal upload clientUploadIpPort {}, DIR {}, fileName {}".format(clientUploadIpPort, DIR, fileName))

    Upload(clientUploadIpPort, DIR, fileName)
    return 1

def downloadPart(port,userAction,userId,DIR,fileName,partNum,chunkSize,numberOfPorts):
    
    
    context = zmq.Context()
    opSocket = context.socket(zmq.REQ)
    opSocket.connect('tcp://'+port)
    opSocket.send_string("{} {} {} {} {} {} {}".format(
        userAction, userId, fileName, partNum, chunkSize, numberOfPorts, getMyIP()+":"+str(clientDownloadPorts[partNum])))
    print("type of  operation and user and file and chunkazes name have been send")
    message = opSocket.recv_string()
    print(message)
   

    # opSocket.send_string()
    print("send ip push pull to datanode port")
    # message = opSocket.recv_string()
    # print(message)
    pullSocket = context.socket(zmq.PULL)
    pullSocket.hwm = 10
    pullSocket.bind("tcp://"+getMyIP()+":"+str(clientDownloadPorts[partNum]))

    fileobj = open(DIR+str(userId)+"_"+str(partNum)+"_"+fileName, 'wb+')
    while True:
        chunk = pullSocket.recv()
        print("asdadasdasdassss")
        #print('data received in '+str(partNum))
        #counter = counter+1
        if chunk is b'':
            print('condition satisfied')
            break
        fileobj.write(chunk)

    fileobj.close()

def SignUp(socket,Port,userName,Email,Password):
   
    getLogger().info(userName)
    getLogger().info(Email)
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    [socket.connect("tcp://%s:%s" % (MASTER_FILESYSTEM_MACHINE_IP,port)) for port in portsdatabaseClients]
    getLogger().info(userName)
    getLogger().info(Email)

    InsertSQL="{} {} {}".format(userName,Email,Password)
    getLogger().info(userName)
    getLogger().info(Email)
   
    socket.send_string(InsertSQL)
    getLogger().info(userName)
    getLogger().info(Email)
   
    userID = socket.recv_string() 
    getLogger().info(userID)
    socket.close()
    return userID 
    
    


if __name__ == "__main__":
    setLoggingFile("Client.log")
    getLogger().info("Client Started")
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    for port in masterClientPorts:
        socket.connect("tcp://%s:%s" % (MASTER_FILESYSTEM_MACHINE_IP, port))
        getLogger().info("Client connected to %s:%s" % (MASTER_FILESYSTEM_MACHINE_IP, port))

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
