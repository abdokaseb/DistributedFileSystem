import mysql.connector
import sys

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  database="lookUpData",
  autocommit=True
)

mycursor = mydb.cursor()

mycursor.execute("CREATE TABLE IF NOT EXISTS Users (UserID Int NOT NULL AUTO_INCREMENT,UserName VARCHAR(30),Email VARCHAR(30) UNIQUE,Pass VARCHAR(25),PRIMARY KEY (UserID));")

# sql = ''
# mainSQL = "INSERT INTO Users (UserName, Email, Pass) VALUES (%s,%s, %s);"

# for i in range(1,20):
#   # sql += mainSQL.format(i)
#   mycursor.execute(mainSQL,('myName','myEmail','myPass'))
# #   mydb.commit()



