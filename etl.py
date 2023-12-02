import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

#load data from S3 (json) to the staging tables
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Inserting data on STG table " + query)
        cur.execute(query)
        conn.commit()


#insert the data transformed from staging tables to the analytics tables (dimension/factual)
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Inserting data on analytic table " + query)
        cur.execute(query)
        conn.commit()


def main():
    #reads the settings from the file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #create the connection with database and a cursos to execute sql commands
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #loading and inserting data on STG and analytics tables
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #close connection with database
    conn.close()


if __name__ == "__main__":
    main()