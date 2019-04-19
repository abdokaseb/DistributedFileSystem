from Util import getMyIP

############################

CHUNK_SIZE = 500
USERACTIONS = {'UPLOAD': 0, 'DOWNLOAD': 1, 'LS': 2}

MASTER_FILESYSTEM_MACHINE_IP = '192.168.137.147'

############ DATABASE
MASTER_DATABASE_MACHINE_IP = '192.168.137.147'
portsSlavesClient = ["5756", "5757", "5758"]
portsdatabaseSlaves = ["8001", "8002", "8003", "8004", "8005", "8006"]
DatabaseportToListenSlaves = "8101"
N_DATABASE_SLAVES=5
############
masterPortUploadSucess = "7010"
masterHeartPort = "5556"

############### replica constanta
MIN_REPLICA_COUNT = 2

###### used as constant in client to access master to send request ( ls ,upload ,download)
masterClientPorts = ["5001", "5002", "5003", "5004", "5005", "5006"]
clientDownloadPorts = ["8501", "8502", "8503", "8504", "8505", "8506"]
portsHandleClentsToSlaves = ["8201", "8202", "8203", "8204", "8205", "8206"]
portsdatabaseClients = ["7001", "7002", "7003", "7004", "7005", "7006"]

########## used in Data node and master
portsDatanodeClient = ["6001", "6002", "6003", "6004", "6005", "6006", "6007"]
portsDatanodeDatanode = ["6101", "6102","6103", "6104", "6105", "6106", "6107"]
defaultAvaliableRepiclaPortsDataNodeDataNode = [str(9000+i) for i in range(20)]



clientUploadIpPort = (getMyIP(), "7005")
