import zmq
import sys
import json

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


portsMasterClient = ["5001","5002","5003","5004","5005","5006"]

context = zmq.Context()
socket = context.socket(zmq.REQ)

for port in portsMasterClient: socket.connect ("tcp://%s:%s" % (MasterTrakerIP,port))

# GET user action, and file name for up and down
userAction = 'UPLOAD'
#file name shouldn't have spaces
fileName = 'py2.py'


if userAction == 'LS':
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
    message = socket.recv_json()
    files = json.loads(message)
    print("the list of files are:")
    for i,file in enumerate(files,start=1): print("\t{}- {}".format(i,file))

elif userAction == 'UPLOAD':
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],''))
    message = socket.recv_string()  #this message is the IP:port for the mechine (e.g 192.168.1.9:5554)
    # communicate with the data node to upload
    print(message)
    if message == "ERROR 404":
        print("Sorry We Are Very Busy")
    pass

elif userAction == 'DOWNLOAD':
    socket.send_string("{} {} {}".format(userID,USERACTIONS[userAction],fileName))
    message = socket.recv_json()  #this message is the array of [IP:port] of size 6 for the mechine (e.g [192.168.1.9:5554,.....,])
    # communicate with the data node to download
    message = json.loads(message)
    print(message)
    if message == "ERROR 404":
        print("Sorry We Are Very Busy")
    pass
    


