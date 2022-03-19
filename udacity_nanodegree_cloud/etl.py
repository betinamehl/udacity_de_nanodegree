import configparser
import psycopg2
import re
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from S3 into staging tables
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param conn: database connection
    :type filepath: postgres (psycopg2) database connection object

    """
    for query in copy_table_queries:
        print("Log: Copying data from s3 into", re.search("copy\s*([^\n\r\s]*)", query).group(1), "table")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables into final tables
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param conn: database connection
    :type filepath: postgres (psycopg2) database connection object

    """
    for query in insert_table_queries:
        print("Log: Inserting data in", re.search("INTO\s*([^\n\r\s]*)", query).group(1), "table")
        cur.execute(query)
        conn.commit()


def main():
    """Connects to the database and call functions for loading data from s3 and inserting data into final tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER-CONNECTION'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()