#import MySQLdb
import mysql.connector
from csv import DictReader,reader
from termcolor import colored

def insert_data(csv_file_addr,mydb,numofrec):
    mycursor = mydb.cursor()
    with open(csv_file_addr,"r",encoding="utf-8") as opener:
        mycursor.execute("SHOW COLUMNS FROM MAINTABLE")
        db_show = mycursor.fetchall()
        db_list = []
        for h in db_show:
            db_list.append(h[0])
        reader = DictReader(opener)
        counter = 0
        id = numofrec
        for row in reader:
            id = id + 1
            value_list = list(row.values())
            value_list.insert(0,id)
            headers = list(row.keys())
            headers.insert(0,"id")
            sorted_value_list = [""]*40
            headers_sort=[""]*40
            sorted_value_list[0] = id
            for j in range(1,len(db_list)):
                for z in range(1,len(headers)):
                    if db_list[j]==headers[z]:
                        sorted_value_list[j]=value_list[z]
                        headers_sort[z]=j
                    else:
                        continue
            mycursor.execute(f"INSERT INTO MAINTABLE (id) VALUES ({sorted_value_list[0]});")
            qinsert = ""
            for i in range(1,len(db_list)):
                qinsert = qinsert + f"{db_list[i]} = '{sorted_value_list[i]}',"
            final_query = f"UPDATE MAINTABLE SET {qinsert.strip(',')} WHERE id={sorted_value_list[0]};"
            try:
                mycursor.execute(final_query)
                mydb.commit()
                print(f"{id}th row added")
            except mysql.connector.Error:
                print(colored(f"Error on row {id}","red"))
    numofrec=id
    return numofrec
def create_table(csv_file_addr,mydb):
    mycursor = mydb.cursor()
    with open(csv_file_addr, "r",encoding="utf-8") as opener:
        reader = DictReader(opener)
        for row in reader:
            pass
        headers = list(row.keys())
        first_part = "CREATE TABLE MAINTABLE ( id INT,"
        column_names=""
        for column in range(len(headers)):
            column_names = column_names + f"{headers[column]}  TEXT,"
        column_query = first_part+ column_names + "primary key (id));"
#        print(column_query)
        mycursor.execute(column_query)
def cheak_add_column(csv_file_addr,mydb):
    mycursor = mydb.cursor()
    with open(csv_file_addr, "r", encoding="utf8") as opener:
        reader = DictReader(opener)
        mycursor.execute("SHOW COLUMNS FROM MAINTABLE")
        db_list = mycursor.fetchall()
        column_names = []
        for j in db_list:
            column_names.append(j[0])
        for row in reader:
            pass
        for i in list(row.keys()):
            if i not in column_names and i != "":
                mycursor.execute(f"ALTER TABLE MAINTABLE\nADD {i} VARCHAR(255);")
            else:
                continue

def add_imported_column(mydb):
     mycursor = mydb.cursor()
     new_columns = ['biblio_imported','authors_imported','authorrelation_imported','taxonomy_imported' ,'taxonomyrelation_imported' ,'links_imported' ,'linksrelation_imported','attributes_imported' ]
     for i in new_columns:
        add_imported_column_query = f"ALTER TABLE MAINTABLE ADD {i} INT;"
        mycursor.execute(add_imported_column_query)
     imported_column_query = "UPDATE MAINTABLE SET biblio_imported=0,authors_imported=0,authorrelation_imported=0,taxonomy_imported=0,taxonomyrelation_imported=0,links_imported=0,linksrelation_imported=0,attributes_imported=0; "
     mycursor.execute(imported_column_query)
     mydb.commit()

def main(mydb,phase,name):
    numofrec=0
    mc = mydb.cursor()
    if  phase=='F' or phase=='f':
        path1 = "/home/madadi/pythoncodes/lib-db/"  # the full path without
        path = path1 + name + ".csv"
        numofrec=0
        create_table(path,mydb)
        numofrec = insert_data(path, mydb, numofrec)
        add_imported_column(mydb)
    elif phase=="R" or phase=="r":
        path1 = "/home/madadi/pythoncodes/lib-db/"  # the full path without
        path = path1 + name + ".csv"
        cheak_add_column(path,mydb)
        mc.execute("SELECT COUNT(*) FROM MAINTABLE;")
        numofrec=mc.fetchall()[0][0]
        insert_data(path,mydb,numofrec)

