import pyodbc

from sql_queries import *

def connection(server, database, uid, pwd):
    try:
        server = server
        database = database
        uid = uid
        pwd = pwd
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+uid+';PWD='+pwd+'')
        cur = conn.cursor()
    except Exception as e:
        print(e)

    return conn, cur

def drop_tables(cur):
    for _ in drop_table_queries:
        cur.execute(_)

def create_tables(cur):
    for _ in create_table_queries:
        cur.execute(_)

def main():
    cur, conn = connection('127.0.0.1', 'sparkifydb', 'sa', 'P@ssw0rd')

    drop_tables(cur)
    create_tables(cur)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
