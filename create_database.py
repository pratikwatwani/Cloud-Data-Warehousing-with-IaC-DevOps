import configparser
import boto3
import pandas as pd
import time
from datetime import datetime
import json
import logging

logging.basicConfig(format='%(asctime)s %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
logging = logging.getLogger(__name__)

start = time.time()
# CONFIG
configFile = 'dwh.cfg'
config = configparser.ConfigParser()
config.read(configFile)

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_REGION             = config.get("DWH", "DWH_REGION")

"""
df = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME", "DWH_REGION"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME,DWH_REGION ]
             })

print(df)
"""

ec2 = boto3.resource('ec2',
                    region_name = 'us-west-2',
                    aws_access_key_id = KEY,
                    aws_secret_access_key = SECRET)

s3 = boto3.resource('s3',
                   region_name = 'us-west-2',
                   aws_access_key_id = KEY,
                   aws_secret_access_key = SECRET)

iam = boto3.client('iam',
                  region_name = 'us-west-2',
                  aws_access_key_id = KEY,
                  aws_secret_access_key = SECRET)

redshift = boto3.client('redshift',
                       region_name = 'us-west-2',
                       aws_access_key_id = KEY,
                       aws_secret_access_key = SECRET)


try:
    print('==========Processing IAM Role==========')
    logging.info('Creating IAM role')
    dwhRole = iam.create_role(
        Path = '/',
        RoleName = DWH_IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on user's behalf",
        AssumeRolePolicyDocument = json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": {
                    "Effect": "Allow",
                    "Principal": {"Service": "redshift.amazonaws.com"},
                    "Action": 'sts:AssumeRole'
                }
            }
        )
    )
    logging.info('IAM role creation complete!')

except Exception as e:
    logging.error(e)
    
    

print('==========Attach Role Policy==========')
logging.info('Attaching role policy to Redshift Cluster')
iam.attach_role_policy(RoleName = DWH_IAM_ROLE_NAME, 
                      PolicyArn = 'arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
logging.info('Policy attached!')


print('==========IAM Role Properties==========')
logging.info('Extracting IAM Role values')
roleArn = iam.get_role(RoleName = DWH_IAM_ROLE_NAME)['Role']['Arn']
logging.info('Extracted IAM role for the cluster!')

try:
    print('==========Redshift Cluster==========')
    logging.info('Creating Redshift cluster')
    
    response = redshift.create_cluster(
        ClusterType = DWH_CLUSTER_TYPE,
        NodeType = DWH_NODE_TYPE,
        NumberOfNodes = int(DWH_NUM_NODES),
        
        DBName = DWH_DB,
        ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,
        MasterUsername = DWH_DB_USER,
        MasterUserPassword = DWH_DB_PASSWORD,
        
        IamRoles = [roleArn]
    )
    logging.info('Cluster creation complete!')
except Exception as e:
    logging.error(e)

    

time.sleep(250)
redshiftProperties = redshift.describe_clusters(ClusterIdentifier = DWH_CLUSTER_IDENTIFIER)['Clusters'][0]   
DWH_ENDPOINT = str(redshiftProperties['Endpoint']['Address'])
DWH_ROLE_ARN = str(redshiftProperties['IamRoles'][0]['IamRoleArn'])

try:
    config.add_section('IAM_ROLE')
    config['DWH']['DWH_ENDPOINT'] = DWH_ENDPOINT
    config['IAM_ROLE']['DWH_ROLE_ARN'] = DWH_ROLE_ARN
    
    with open(configFile, 'w') as f:
        config.write(f)
        
except Exception as e:
    logging.error(e)



try:
    print('==========VPC Access==========')
    logging.info('Creatig VPC security group and access')
    vpc = ec2.Vpc(id = redshiftProperties['VpcId'])
    
    defaultSg = list(vpc.security_groups.all())[0]
    
    defaultSg.authorize_ingress(
        GroupName = defaultSg.group_name,
        CidrIp = '0.0.0.0/0',
        IpProtocol = 'TCP',
        FromPort = int(DWH_PORT),
        ToPort = int(DWH_PORT)
    )
    logging.info('VPC creation complete!')
except Exception as e:
    logging.error(e)

end = time.time()-start
print("Total time utilized {}".format(end))
