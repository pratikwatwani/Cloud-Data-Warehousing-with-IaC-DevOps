import configparser
import psycopg2
from create_sql_queries import copy_table_queries, insert_table_queries
import logging
import time

logging.basicConfig(format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)
logging = logging.getLogger(__name__)

def load_staging_tables(cur, conn):
    logging.info('Transfering data to the staging area')
    for query in copy_table_queries:
        t0 = time.time()
        print('-----Executing query for: {}'.format(query.split(' ')[5]))
        cur.execute(query)
        conn.commit()
        print('Total time to commit: {:.2f} seconds'.format(time.time()-t0))


def insert_tables(cur, conn):
    logging.info('Transferring data to fact and dimension tables')
    for query in insert_table_queries:
        t0 = time.time()
        print('-----Executing query for: {}'.format(query.split(' ')[2]))
        cur.execute(query)
        conn.commit()
        print('Total time to commit: {:.2f} seconds'.format(time.time()-t0))


def main():
    import configparser
    config = configparser.ConfigParser()
    config.read(r'./dwh.cfg')

    host = config['DWH']['dwh_endpoint']
    dbname = config['DWH']['dwh_db'] 
    user = config['DWH']['dwh_db_user'] 
    password = config['DWH']['dwh_db_password'] 
    port = config['DWH']['dwh_port']
    
    logging.info('Establishing connection to the database')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, dbname, user, password, port))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()