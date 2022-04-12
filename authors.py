import mysql.connector
from termcolor import colored

def create_table(mydb):
    mycursor = mydb.cursor()
    query_authors = "CREATE TABLE AUTHORS ( id INT PRIMARY KEY AUTO_INCREMENT,author_unique VARCHAR(255),name VARCHAR(255),biography TEXT,picture BLOB);"
    query_authorrelation = "CREATE TABLE AUTHORRELATION ( id INT PRIMARY KEY AUTO_INCREMENT,bookid VARCHAR(255),authorid VARCHAR(255),relator VARCHAR(255));"

#    print(query)
    mycursor.execute(query_authors)
    print("authors created")
    mycursor.execute(query_authorrelation)
    print("authorrelation created")

def insert_data(mydb,record_number):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT COUNT(*) FROM MAINTABLE;")
    count=int("".join(str(list(mycursor.fetchall())[0])).strip(",)").strip("("))
#    print(count)
    mycursor = mydb.cursor()
    counter_author_id = 0
    for i in range(record_number,count+1):
        mycursor.execute(f"SELECT authors_author,authors_translator,author_unique,id FROM MAINTABLE WHERE id ={i};")
        each_row=mycursor.fetchall()
        mycursor.execute(f"SELECT name from AUTHORS")
        exsisting_author_unique = [list(z) for z in (mycursor.fetchall())]
        if each_row[0][0] != "" :
            try:
                each_author_list = each_row[0][0].split("|")
            except:
                pass
            for author in each_author_list:
                author_l = [author]
                if author_l in exsisting_author_unique :
                    query_authorrelation = f"INSERT INTO AUTHORRELATION (id,bookid,authorid,relator) VALUES (default,'{i}','{counter_author_id}','author');"
                    mycursor.execute(query_authorrelation)
                    mydb.commit()
                    continue
                else:
                    query_author = f"INSERT INTO AUTHORS (id,author_unique,name,biography,picture) VALUES (default,null,'{author}',null,null);"
                    mycursor.execute(query_author)
                    mydb.commit()
                    counter_author_id = counter_author_id + 1
                    query_authorrelation = f"INSERT INTO AUTHORRELATION (id,bookid,authorid,relator) VALUES (default,'{i}','{counter_author_id}','author');"
                    mycursor.execute(query_authorrelation)
                    mydb.commit()

        mycursor.execute(f"SELECT name from AUTHORS")
        exsisting_author_unique = [list(t) for t in (mycursor.fetchall())]
        if each_row[0][1] != "":
            try:
                each_translator_list = each_row[0][1].split("|")
            except:
                pass
            for translator in each_translator_list:
                translator_l = [translator]
                if translator_l in exsisting_author_unique:
                    query_authorrelation = f"INSERT INTO AUTHORRELATION (id,bookid,authorid,relator) VALUES (default,'{i}','{counter_author_id}','translator');"
                    mycursor.execute(query_authorrelation)
                    mydb.commit()
                    continue
                else:
                    query_author = f"INSERT INTO AUTHORS (id,author_unique,name,biography,picture) VALUES (default,null,'{translator}',null,null);"
                    mycursor.execute(query_author)
                    mydb.commit()
                    counter_author_id = counter_author_id + 1
                    query_authorrelation = f"INSERT INTO AUTHORRELATION (id,bookid,authorid,relator) VALUES (default,'{i}','{counter_author_id}','translator');"
                    mycursor.execute(query_authorrelation)
                    mydb.commit()
        imported_column_query = f"UPDATE MAINTABLE SET authors_imported=1,authorrelation_imported=1 WHERE id={i}; "
        mycursor.execute(imported_column_query)
        print(f"record of {i} from main table added to new tables(AUTHORS,AUTHORRELATION)")
    print("*****************************************************************************************************************************")

def main(mydb):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT id FROM MAINTABLE WHERE authorrelation_imported=1; ")
    try:
        record_number = int(mycursor.fetchall()[-1][0]) + 1
    except:
        record_number = 1
    if record_number == 1 :
        create_table(mydb)
    else:
        pass
    insert_data(mydb,record_number)
