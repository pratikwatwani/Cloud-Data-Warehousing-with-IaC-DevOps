import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logging = logging.getLogger(__name__)

def drop_tables(cur, conn):
    logging.info('Executing drop statements to drop tables if they exist')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    logging.info('Executing create statements to create tables')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    import configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
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