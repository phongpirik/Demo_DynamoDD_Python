import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="phongpirika5"
)

mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE mydatabase")

mycursor.execute("SHOW DATABASES")
for x in mycursor:
  print(x)