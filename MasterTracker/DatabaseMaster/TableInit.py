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
  database="lookUpData",
  autocommit=True
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE IF NOT EXISTS Users (UserID Int, UserName VARCHAR(30), Email VARCHAR(30), Pass VARCHAR(25))")

# sql = ''
# mainSQL = "INSERT INTO Users (UserID, UserName, Email, Pass) VALUES (%s,%s,%s, %s);"

# for i in range(1,20):
#   # sql += mainSQL.format(i)
#   mycursor.execute(mainSQL,(str(i),'myName','myEmail','myPass'))
# #   mydb.commit()



