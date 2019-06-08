"""
Project Description:
"""
import psycopg2

def get_details():
    with open("data.txt", 'r') as r1:
        host = r1.readline().split("=")[1].strip()
        db = r1.readline().split("=")[1].strip()
        user = r1.readline().split("=")[1].strip()
        pwd = r1.readline().split("=")[1].strip()
        r1.close()
    return (host, db, user, pwd)

# Connect to an existing database
def connect_database():
    details = get_details()
    conn = psycopg2.connect(host=details[0], database=details[1], user=details[2], password=details[3])
    return conn

# Open a cursor to perform database operations
def start_transaction(conn):
    cur = conn.cursor()
    return cur

# Create table function
def construct_case_table(cur, table_name, primary_attribute, primary_type, data_type):
    drop_table_query ="""DROP TABLE IF EXISTS %s""" % table_name
    
    cur.execute(drop_table_query)
    
    create_table_query = """CREATE TABLE %s(
                %s %s PRIMARY KEY,""" % (table_name, primary_attribute, primary_type)
    for k in data_type:
        if data_type[k] == 'list':
            create_table_query = create_table_query + "\n" + k.replace('-', '_') + " text[],"
        else:
            create_table_query = create_table_query + "\n" + k.replace('-', '_') + " text,"
    create_table_query = create_table_query[:-1] + ")"
    
    cur.execute(create_table_query)

# Insert into table function
def insert_value(cur, table_name, attribute_length, attributes):
    insert_query = """
        INSERT INTO %s
        VALUES(""" % table_name

    add_values = """%s"""
    for i in range(attribute_length):
        add_values += ",%s"
    add_values += ")"
    insert_query += add_values
    #print(attributes)
    cur.execute(insert_query, attributes)
    
        
#Delete table
def delete_table(cur,table):
    delete_query = "DROP TABLE " + table
    cur.execute(delete_query)

# Make the changes to the database persistent
def commit_transaction(conn):
    conn.commit()

#Undo changes of current transaction to the DB
def rollback_transaction(conn):
    conn.rollback()
    
# Close communication with the database (cursor & DB connection)
def end_transaction(cur,conn):
    cur.close()
    conn.close()
