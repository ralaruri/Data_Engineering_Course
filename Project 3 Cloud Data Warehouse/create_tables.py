import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
"""
    This function deletes the tables that already exist for the ones will we create.
"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
"""
    This function creates the tables we need to create our full database, including staging and final tables we query off of for Sparkify.
    We are creating the skeletons of the tables (names of columns and types of columns), organzing our sort and dist keys.
"""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
"""
    This function allows us to connect to our datawarehouse with our inputs we added in our DWH.cfg file (passwords,cluster, port number etc.)
    then it executes the drop table function proceeded by the create_table function. 
    This is helpful when debugging we need to recreate the tables mutiple times when we need to improve query perofrmance or change sort/dist keys potentially. 
"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()