import configparser
import psycopg2
import re
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables

    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param conn: database connection
    :type filepath: postgres (psycopg2) database connection object

    """
    for query in drop_table_queries:
        print("Log: Dropping", re.search("EXISTS\s*([^\n\r\s]*)", query).group(1), "table")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables
    
    :param cur: database cursor
    :type cur:  postgres (psycopg2) database cursor object
    :param conn: database connection
    :type filepath: postgres (psycopg2) database connection object

    """
    for query in create_table_queries:
        print("Log: Creating", re.search("EXISTS\s*([^\n\r\s]*)", query).group(1), "table")
        cur.execute(query)
        conn.commit()


def main():
    """Connects to the database and call functions for dropping and creating tables"""

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER-CONNECTION'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()