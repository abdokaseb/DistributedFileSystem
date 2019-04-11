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

mycursor.execute("CREATE TABLE IF NOT EXISTS machines (ID Int, IP VARCHAR(13), primary key(ID))")
mycursor.execute("CREATE TABLE IF NOT EXISTS files (UserID Int, machID Int, fileName VARCHAR(255))")

# sql = ''
mainSQL = "INSERT INTO machines (ID,IP) VALUES (%s, %s);"

for i in range(10):
  # sql += mainSQL.format(i)
  mycursor.execute(mainSQL,(str(i),'0'))
  mydb.commit()



