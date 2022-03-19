import boto3
import json
import configparser
import time
from datetime import datetime
from botocore.exceptions import ClientError

config_file = 'dwh.cfg'
config = configparser.ConfigParser()
config.optionxform = str
config.read_file(open(config_file))


#IMPORTING VARIABLES
KEY                     = config.get('AWS','KEY')
SECRET                  = config.get('AWS','SECRET')

CLUSTER_TYPE            = config.get("CLUSTER-CREATION","CLUSTER_TYPE")
NUM_NODES               = config.get("CLUSTER-CREATION","NUM_NODES")
NODE_TYPE               = config.get("CLUSTER-CREATION","NODE_TYPE")

CLUSTER_IDENTIFIER      = config.get("CLUSTER-CREATION","CLUSTER_IDENTIFIER")
DB_NAME                 = config.get("CLUSTER-CONNECTION","DB_NAME")
DB_USER                 = config.get("CLUSTER-CONNECTION","DB_USER")
DB_PASSWORD             = config.get("CLUSTER-CONNECTION","DB_PASSWORD")
DB_PORT                 = config.get("CLUSTER-CONNECTION","DB_PORT")

IAM_ROLE_NAME           = config.get("CLUSTER-CREATION", "IAM_ROLE_NAME")


# CREATING CLIENTS FOR EC2, S3, IAM AND REDSHIFT

s3 = boto3.resource('s3',
                    region_name='us-west-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET)

iam = boto3.client('iam',
                    region_name='us-west-2',
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET) 

redshift = boto3.client('redshift',
                        region_name='us-west-2',
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET) 


# CREATING IAM ROLE

role_policy_document = json.dumps(
                                {'Statement': [{ 
                                                'Action': 'sts:AssumeRole',
                                                'Effect': 'Allow',
                                                'Principal': {'Service': 'redshift.amazonaws.com'}
                                                }],
                                'Version':'2012-10-17'
                                }
)

try:
    print("Creating a new IAM Role") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=role_policy_document
    )    
except Exception as e:
    print(e)
    
    
print("Attaching Policy")

iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("Get the IAM role ARN")
role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']


# SAVING IAM ROLE IN CONFIG FILE
# CODE STRUCTURE FROM USER ARUN IN STACKOVERFLOW (https://stackoverflow.com/questions/44321825/editing-the-configuration-file-using-python-script)

try: 
    if(config.has_section('IAM_ROLE')):
        config.set('IAM_ROLE', 'ARN', f'{role_arn}')
        with open(config_file, "w") as conf_file:
            config.write(conf_file, space_around_delimiters=False)
            print()
except Exception as e:
    print(e)


# CREATE REDSHIFT CLUSTER

try:
    response = redshift.create_cluster(ClusterType=CLUSTER_TYPE,
                                       NodeType=NODE_TYPE,
                                       NumberOfNodes=int(NUM_NODES),
                                       DBName=DB_NAME,
                                       ClusterIdentifier=CLUSTER_IDENTIFIER,
                                       MasterUsername=DB_USER,
                                       MasterUserPassword=DB_PASSWORD,
                                       IamRoles=[role_arn]
                                      )
except Exception as e:
    print(e)

t_end = time.time() + 60 * 15

while time.time() <t_end:
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    print(f'[{datetime.now()}] ',"Creating Cluster...")
    if myClusterProps['ClusterStatus'] == 'available':
        print("Cluster created and available")
        break
    time.sleep(30)
else: 
    print("cluster took more than 15 minutes to create")

redshift_endpoint = myClusterProps['Endpoint']['Address']


# SAVING REDSHIFT INFO IN THE CONFIG FILE
# CODE STRUCTURE FROM USER ARUN IN STACKOVERFLOW (https://stackoverflow.com/questions/44321825/editing-the-configuration-file-using-python-script)

try: 
    if(config.has_section('CLUSTER-CONNECTION')):
        config.set('CLUSTER-CONNECTION', 'DB_HOST', f'{redshift_endpoint}')
        with open(config_file, "w") as conf_file:
            config.write(conf_file, space_around_delimiters=False)
            print()
except Exception as e:
    print(e)  
