import logging

def getMyIP():
    if(getMyIP.ip==''):
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        getMyIP.ip = s.getsockname()[0]
        s.close()
    return getMyIP.ip
getMyIP.ip=''

def setLoggingFile(fileName):
    logging.basicConfig(filename='./logs/'+fileName, filemode='a', format='%(name)s - %(levelname)s - %(message)s',level="INFO")
    logging.info("garabe") # to create fiel
    with open("./logs/"+fileName,'w') as f:
        clearFile=1;
    
def getLogger():
    return logging





