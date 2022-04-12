import mysql.connector
from termcolor import colored

def create_table(mydb):
    mycursor = mydb.cursor()


    query_biblio = "CREATE TABLE BIBLIO ( id INT PRIMARY KEY AUTO_INCREMENT,sku VARCHAR(255),importdate TIMESTAMP,lastmodification VARCHAR(255));"
    query_taxonomy = "CREATE TABLE TAXONOMY ( id INT PRIMARY KEY AUTO_INCREMENT,value VARCHAR(255),persianname VARCHAR(255),type VARCHAR(255));"
    query_taxonomyrelation = "CREATE TABLE TAXONOMYRELATION ( id INT PRIMARY KEY AUTO_INCREMENT,bookid VARCHAR(255),taxonomyid VARCHAR(255),relation VARCHAR(255));"

#    print(query)
    mycursor.execute(query_biblio)
    print("biblio created")
    mycursor.execute(query_taxonomy)
    print("taxonomy created")
    mycursor.execute(query_taxonomyrelation)
    print("taxonomyrelation created")

def insert_data(mydb,record_number):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT COUNT(*) FROM MAINTABLE;")
    count=int("".join(str(list(mycursor.fetchall())[0])).strip(",)").strip("("))
#    print(count)
    mycursor = mydb.cursor()
    counter_taxonomy_id = 0
    for i in range(record_number,count+1):
        mycursor.execute(f"SELECT biblio_sku,taxonomy_tag,taxonomy_category,taxonomy_collection,taxonomy_material FROM MAINTABLE WHERE id ={i};")
        each_row=mycursor.fetchall()
        query_biblio = f"INSERT INTO BIBLIO (id,sku,importdate,lastmodification) VALUES (default,'{each_row[0][0]}',CURRENT_TIMESTAMP,null);"
        mycursor.execute(query_biblio)
        mydb.commit()
        if each_row[0][1] != "" :
            try:
                each_tag_list = each_row[0][1].split("|")
            except:
                pass
            for tag in each_tag_list:
                query_taxonomy = f"INSERT INTO TAXONOMY (id,value,persianname,type) VALUES (default,'{tag}',null,'tag');"
                mycursor.execute(query_taxonomy)
                mydb.commit()
                counter_taxonomy_id = counter_taxonomy_id + 1 
                query_taxonomyrelation = f"INSERT INTO TAXONOMYRELATION (id,bookid,taxonomyid,relation) VALUES (default,'{i}','{counter_taxonomy_id}','tag');"
                mycursor.execute(query_taxonomyrelation)
                mydb.commit()
        if each_row[0][2] != "" :
            try:
                each_category_list = each_row[0][2].split("|")
            except:
                pass
            for category in each_category_list:
                query_taxonomy = f"INSERT INTO TAXONOMY (id,value,persianname,type) VALUES (default,'{category}',null,'category');"
                mycursor.execute(query_taxonomy)
                mydb.commit()
                counter_taxonomy_id = counter_taxonomy_id + 1 
                query_taxonomyrelation = f"INSERT INTO TAXONOMYRELATION (id,bookid,taxonomyid,relation) VALUES (default,'{i}','{counter_taxonomy_id}','category');"
                mycursor.execute(query_taxonomyrelation)
                mydb.commit()
        if each_row[0][3] != "" :
            try:
                each_collection_list = each_row[0][3].split("|")
            except:
                pass 
            for collection in each_collection_list:
                query_taxonomy = f"INSERT INTO TAXONOMY (id,value,persianname,type) VALUES (default,'{collection}',null,'collection');"
                mycursor.execute(query_taxonomy)
                mydb.commit()
                counter_taxonomy_id = counter_taxonomy_id + 1 
                query_taxonomyrelation = f"INSERT INTO TAXONOMYRELATION (id,bookid,taxonomyid,relation) VALUES (default,'{i}','{counter_taxonomy_id}','collection');"
                mycursor.execute(query_taxonomyrelation)
                mydb.commit()
        if each_row[0][4] != "" :
            try:
                each_material_list = each_row[0][4].split("|")
            except:
                pass
            for material in each_material_list:
                query_taxonomy = f"INSERT INTO TAXONOMY (id,value,persianname,type) VALUES (default,'{material}',null,'material');"
                mycursor.execute(query_taxonomy)
                mydb.commit()
                counter_taxonomy_id = counter_taxonomy_id + 1 
                query_taxonomyrelation = f"INSERT INTO TAXONOMYRELATION (id,bookid,taxonomyid,relation) VALUES (default,'{i}','{counter_taxonomy_id}','material');"
                mycursor.execute(query_taxonomyrelation)
                mydb.commit()
        imported_column_query = f"UPDATE MAINTABLE SET biblio_imported=1,taxonomy_imported=1,taxonomyrelation_imported=1 WHERE id={i}; "
        mycursor.execute(imported_column_query)
        print(f"record of {i} from main table added to new tables(BIBLIO,TAXONOMY,TAXONOMYRELATION)")
    print("*****************************************************************************************************************************")

def main(mydb):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT id FROM MAINTABLE WHERE taxonomyrelation_imported=1; ")
    try:
        record_number = int(mycursor.fetchall()[-1][0]) + 1
    except:
        record_number = 1
    if record_number == 1 :
        create_table(mydb)
    else:
        pass
    insert_data(mydb,record_number)
