import create_tables
import etl
import sql_queries

import logging
logging.basicConfig(format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logging = logging.getLogger(__name__)


def main():
    logging.info('Running create_databse.py')
    exec(open('create_database.py').read())
    
    logging.info('Running created_tables.py')
    exec(open('create_tables.py').read())
    
    logging.info('Running etl.py')
    exec(open('etl.py').read())

if __name__ == "__main__":
    logging.info('Initializing program')
    main()