import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

"""Remove all table if exits"""


def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("Proccess DROP query: " + query)
        cur.execute(query)
        conn.commit()


"""Create list table"""


def create_tables(cur, conn):
    for query in create_table_queries:
        print("Proccess CREATE query: " + query)
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

    print("Start execute DROP query...")
    drop_tables(cur, conn)
    print("Start execute CREATE query...")
    create_tables(cur, conn)
    print("Done.")
    conn.close()


if __name__ == "__main__":
    main()
