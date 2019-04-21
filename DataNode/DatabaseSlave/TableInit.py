import mysql.connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from Constants import SLAVE_DATABASE_HOST,SLAVE_DATABASE_USER,SLAVE_DATABASE_PASSWORD,SLAVE_DATABASE_DATABASE

mydb = mysql.connector.connect(
  host=SLAVE_DATABASE_HOST,
  user=SLAVE_DATABASE_USER,
  passwd=SLAVE_DATABASE_PASSWORD
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS "+SLAVE_DATABASE_DATABASE)


mydb = mysql.connector.connect(
  host=SLAVE_DATABASE_HOST,
  user=SLAVE_DATABASE_USER,
  passwd=SLAVE_DATABASE_PASSWORD,
  database=SLAVE_DATABASE_DATABASE,
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



