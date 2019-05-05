from Util import getMyIP

############################
DIR = "./DataNode/videos/"
CHUNK_SIZE = 500
USERACTIONS = {'UPLOAD': 0, 'DOWNLOAD': 1, 'LS': 2}

MASTER_FILESYSTEM_MACHINE_IP = '192.168.1.10'

############ DATABASE
MASTER_DATABASE_MACHINE_IP = '192.168.1.10'
DNS_MACHINE_IP = '192.168.1.10'
N_DATABASE_SLAVES=5
portsSlavesClient = list(range(13300,13307))
portsdatabaseSlaves = list(range(13310,13310+N_DATABASE_SLAVES))
DatabaseportToListenSlaves = "13320"

############ DATABASE ENTRY
MASTER_TRAKER_HOST="localhost"
MASTER_TRAKER_USER="root"
MASTER_TRAKER_PASSWORD=""
MASTER_TRAKER_DATABASE="lookUpData"

MASTER_DATABASE_HOST="localhost"
MASTER_DATABASE_USER="root"
MASTER_DATABASE_PASSWORD=""
MASTER_DATABASE_DATABASE="lookUpDataMaster"

SLAVE_DATABASE_HOST="localhost"
SLAVE_DATABASE_USER="root"
SLAVE_DATABASE_PASSWORD=""
SLAVE_DATABASE_DATABASE="lookUpDataSlave"

############
masterPortUploadSucess = "13321"
masterHeartPort = "13322"

############### replica constanta
MIN_REPLICA_COUNT = 3

MAX_NUMBER_MACHINES = 20

###### used as constant in client to access master to send request ( ls ,upload ,download)
numberOfPortsToDownload = 7
clientDownloadPortsMin = 13420
clientDownloadPortsMax = 13620
masterClientPorts = list(range(13330,13337))
# clientDownloadPorts = list(range(13340,13340+numberOfPortsToDownload))
portsHandleClentsToSlaves = list(range(13350,13357))
portsdatabaseClients = list(range(13360,13367))

########## used in Data node and master
portsDatanodeClient = list(range(13370,13377))
portsDatanodeDatanode = list(range(13380,13387))
defaultAvaliableRepiclaPortsDataNodeDataNode = list(range(13390,13410))


clientUploadIpPort = (getMyIP(), "13323")
