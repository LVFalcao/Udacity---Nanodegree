import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

#delete tables, if they already exist. This allows you to run the script n times
def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("Deleting the table " + query)
        cur.execute(query)
        conn.commit()

#DDL - create the tables
def create_tables(cur, conn):
    for query in create_table_queries:
        print("Creating the table " + query)
        cur.execute(query)
        conn.commit()


def main():
    #reads the settings from the file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #create the connection with database and a cursos to execute sql commands
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # set up tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #close connection with database
    conn.close()


if __name__ == "__main__":
    main()