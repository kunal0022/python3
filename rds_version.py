import boto3
import configparser
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

config = configparser.ConfigParser()
#config.read('.config_key_s3')
config.read('/app/automation/keys/.config.ini')

def rds_details(account_id):
    def get_rds_instances(region, access_key, secret_key):
        client = boto3.client('rds', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        return client.describe_db_instances().get('DBInstances', [])

    aws_access_key_id = config[account_id]['aws_access_key_id']
    aws_secret_access_key = config[account_id]['aws_secret_access_key']
    region = config[account_id]['region']

    for instance in get_rds_instances(region, aws_access_key_id, aws_secret_access_key):
        instance_name = instance['DBInstanceIdentifier']
        instance_version = instance['EngineVersion']
        instance_node_type = instance['DBInstanceClass']
        instance_engine = instance['Engine']
        print(f"{account_id},{instance_name},{instance_version},{instance_node_type},{instance_engine}")

if __name__ == "__main__":
    print("Account, RDS Instance, Version, Node Details, Database Engine")
    rds_details("itx-acm")




