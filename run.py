import mysql.connector
import biblio,links_attributes,authors,original
import sys

mydb = mysql.connector.connect(
    host="192.168.18.128",
    user="root",
    password="password",
    database="LIBRARY",
    charset="utf8"
)
original.main(mydb,sys.argv[1],sys.argv[2])
biblio.main(mydb)
authors.main(mydb)
links_attributes.main(mydb)
