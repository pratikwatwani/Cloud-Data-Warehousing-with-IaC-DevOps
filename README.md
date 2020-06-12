# Cloud Data Warehousing with IaC 

## Description
Building an ETL pipeline that extracts data from Amazon Web Services S3, stages them in Redshift Cluster, and transforms data into a set of dimensional tables for the analytics teams. The Redshift cluster is created and maintained using the Infrastructure as Code paradigm and policies.

## Directory Structure

    ├── README.md
    ├── dwh.cfg
    ├── run.py
    ├── modules
      └── create_database.py
      └── create_tables.py
      └── sql_queries.py
      └── etl.py
      └── __init__.py
      
    
        
