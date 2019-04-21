import mysql.connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Constants import MASTER_TRAKER_HOST, MASTER_TRAKER_USER, MASTER_TRAKER_PASSWORD, MASTER_TRAKER_DATABASE


mydb = mysql.connector.connect(
  host=MASTER_TRAKER_HOST,
  user=MASTER_TRAKER_USER,
  passwd=MASTER_TRAKER_PASSWORD
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS "+MASTER_TRAKER_DATABASE)


mydb = mysql.connector.connect(
  host=MASTER_TRAKER_HOST,
  user=MASTER_TRAKER_USER,
  passwd=MASTER_TRAKER_PASSWORD,
  database=MASTER_TRAKER_DATABASE
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE IF NOT EXISTS machines (ID Int, isAlive Int, IP INT(4) UNSIGNED, primary key(ID))")
mycursor.execute("CREATE TABLE IF NOT EXISTS files (UserID Int, machID Int, fileName VARCHAR(255))")

# sql = ''
try:
  mainSQL = "INSERT INTO machines (ID,IP, isAlive) VALUES (%s,INET_ATON(%s), %s);"
  for i in range(1,21):
    # sql += mainSQL.format(i)
    mycursor.execute(mainSQL,(str(i),'0.0.0.0','0'))
    mydb.commit()
except:
  pass


