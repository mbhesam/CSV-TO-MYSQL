import mysql.connector
from time import sleep
from termcolor import colored
def create_table(mydb):
    mycursor = mydb.cursor()
    query_links = "CREATE TABLE LINKS ( id INT PRIMARY KEY AUTO_INCREMENT,sku VARCHAR(255),urllink TEXT,caption TEXT,domain VARCHAR(255),type VARCHAR(255));"
    query_linksrelation = "CREATE TABLE LINKSRELATION ( id INT PRIMARY KEY AUTO_INCREMENT,bookid VARCHAR(255),linksid VARCHAR(255),order_of VARCHAR(255),typeofrelation VARCHAR(255));"
    query_attributes = "CREATE TABLE ATTRIBUTES ( id INT PRIMARY KEY AUTO_INCREMENT,bookid VARCHAR(255),attributekey VARCHAR(255),attributevalue TEXT);"

#    print(query)
    mycursor.execute(query_links)
    print("links created")
    mycursor.execute(query_linksrelation)
    print("linksrelation created")
    mycursor.execute(query_attributes)
    print("attributes created")

def insert_data(mydb,record_number):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT COUNT(*) FROM MAINTABLE;")
    count=int("".join(str(list(mycursor.fetchall())[0])).strip(",)").strip("("))
#    print(count)
    mycursor = mydb.cursor()
    counter_link_id = 0
    for i in range(record_number,count+1):
        mycursor.execute(f"SELECT attribute_abstract,attribute_size,attribute_language,attribute_extention,attribute_pagenumber,attribute_publisher,links_thumbnail,links_download,biblio_sku FROM MAINTABLE WHERE id ={i};")
        each_row=mycursor.fetchall()
        attribute_list = ["abstract" , "size" , "language","extension","pagen_umber","publisher"]
        for x in range(6):
            try:
                query_attribute = f"INSERT INTO ATTRIBUTES (id,bookid,attributekey,attributevalue) VALUES (default,'{i}','{attribute_list[x]}','{each_row[0][x]}');"
                mycursor.execute(query_attribute)
                mydb.commit()
            except:
                print(colored(f"ERROR while adding attribute {attribute_list[x]} with bookid {i}","red"))
        if each_row[0][6] != "":
            try:
                each_tumbnail_link = each_row[0][6].split("|")
                counter_each_link = 0
                for link in each_tumbnail_link:
                    counter_each_link = counter_each_link + 1
                    each_tumbnail_part = link.split("**")
                    if len(each_tumbnail_part)==2:
                        mycursor.execute(f"INSERT INTO LINKS (id,sku,urllink,caption,domain,type) VALUES (default,'{each_row[0][8]}','{each_tumbnail_part[0]}','{each_tumbnail_part[1]}',null,'thumbnail');")
                        mydb.commit()
                        counter_link_id = counter_link_id + 1
                        mycursor.execute(f"INSERT INTO LINKSRELATION (id,bookid,linksid,order_of,typeofrelation) VALUES (default,'{i}','{counter_link_id}','{counter_each_link}','thumbnail');")
                    else:
                        mycursor.execute(f"INSERT INTO LINKS (id,sku,urllink,caption,domain,type) VALUES (default,'{each_row[0][8]}','{each_tumbnail_part[0]}',null,null,'thumbnail');")
                        mydb.commit()
                        counter_link_id = counter_link_id + 1
                        mycursor.execute(f"INSERT INTO LINKSRELATION (id,bookid,linksid,order_of,typeofrelation) VALUES (default,'{i}','{counter_link_id}','{counter_each_link}','thumbnail');")
            except:
                pass

        if each_row[0][7] != "":
            try:
                each_tumbnail_link = each_row[0][7].split("|")
                counter_each_link = 0
                for link in each_tumbnail_link:
                    counter_each_link = counter_each_link + 1
                    each_tumbnail_part = link.split("**")
                    if len(each_tumbnail_part) == 2:
                        mycursor.execute(f"INSERT INTO LINKS (id,sku,urllink,caption,domain,type) VALUES (default,'{each_row[0][8]}','{each_tumbnail_part[0]}','{each_tumbnail_part[1]}',null,'download');")
                        mydb.commit()
                        counter_link_id = counter_link_id + 1
                        mycursor.execute(f"INSERT INTO LINKSRELATION (id,bookid,linksid,order_of,typeofrelation) VALUES (default,'{i}','{counter_link_id}','{counter_each_link}','download');")
                    else:
                        mycursor.execute(f"INSERT INTO LINKS (id,sku,urllink,caption,domain,type) VALUES (default,'{each_row[0][8]}','{each_tumbnail_part[0]}',null,null,'download');")
                        mydb.commit()
                        counter_link_id = counter_link_id + 1
                        mycursor.execute(f"INSERT INTO LINKSRELATION (id,bookid,linksid,order_of,typeofrelation) VALUES (default,'{i}','{counter_link_id}','{counter_each_link}','download');")
            except:
                pass
        imported_column_query = f"UPDATE MAINTABLE SET links_imported=1,linksrelation_imported=1,attributes_imported=1 WHERE id={i}; "
        mycursor.execute(imported_column_query)
        mydb.commit()
        print(f"record of {i} from main table added to new tables(ATTRIBUTES,LINKS,LINKSRELATION)")
    print("*****************************************************************************************************************************")
def main(mydb):
    mycursor = mydb.cursor()
    mycursor.execute("SELECT id FROM MAINTABLE WHERE linksrelation_imported=1; ")
    try:
        record_number = int(mycursor.fetchall()[-1][0]) + 1
    except:
        record_number = 1
    if record_number == 1 :
        create_table(mydb)
    else:
        pass
    insert_data(mydb,record_number)
