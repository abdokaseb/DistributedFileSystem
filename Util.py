def getMyIP():
    if(getMyIP.ip==''):
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        getMyIP.ip = s.getsockname()[0]
        s.close()
    return getMyIP.ip
getMyIP.ip=''
