

def getMyIP():
    import socket
    return socket.gethostbyname(socket.gethostname())
