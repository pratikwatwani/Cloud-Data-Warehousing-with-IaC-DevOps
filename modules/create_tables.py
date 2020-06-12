
import configparser
import psycopg2
from create_sql_queries import drop_table_queries, create_table_queries
import logging
import os


logging.basicConfig(format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging = logging.getLogger(__name__)

def drop_tables(cur, conn):
    logging.info('Dropping existing tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    logging.info('Creating new tables')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    import configparser
    config = configparser.ConfigParser()
    config.read(r'./dwh.cfg')
    
    host = config['DWH']['dwh_endpoint']
    dbname = config['DWH']['dwh_db'] 
    user = config['DWH']['dwh_db_user'] 
    password = config['DWH']['dwh_db_password'] 
    port = config['DWH']['dwh_port']
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()