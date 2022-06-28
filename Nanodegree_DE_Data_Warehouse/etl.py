import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


"""Query load data from AWS S3 to Redshift"""


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Loading from S3: '+query)
        cur.execute(query)
        conn.commit()


"""Transform data from staging tables to fact and dimension tables"""


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Transform by: '+query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("Start connecting to the AWS Redshift ...")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(
        *config['CLUSTER'].values()))
    print("AWS Redshift connected!")
    cur = conn.cursor()

    print("Start loading staging tables...")
    load_staging_tables(cur, conn)
    print("Start loading transform from staging tables to star schema...")
    insert_tables(cur, conn)
    print("Done.")
    conn.close()


if __name__ == "__main__":
    main()
