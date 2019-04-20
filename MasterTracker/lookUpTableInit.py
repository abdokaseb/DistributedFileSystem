import mysql.connector
import sys

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd=""
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS lookUpData")


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="lookUpData"
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


