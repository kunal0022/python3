import boto3
import configparser
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

config = configparser.ConfigParser()
#config.read('.config_key_s3')
config.read('/app/automation/keys/.config.ini')

def redshift_details(account_id):
    def get_redshift_clusters(region, access_key, secret_key):
        client = boto3.client('redshift', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        return client.describe_clusters(ClusterIdentifier='')['Clusters']

    aws_access_key_id = config[account_id]['aws_access_key_id']
    aws_secret_access_key = config[account_id]['aws_secret_access_key']
    region = config[account_id]['region']

    for cluster in get_redshift_clusters(region, aws_access_key_id, aws_secret_access_key):
        #print(cluster)
        cluster_name = cluster['ClusterIdentifier']
        cluster_version = cluster['ClusterVersion']
        node_type = cluster['NodeType']
        num_nodes = cluster['NumberOfNodes']
        database_engine = 'Redshift'
        print(f"{account_id},{cluster_name},{cluster_version},{node_type},{num_nodes},{database_engine}")

if __name__ == "__main__":
    print("Account, Redshift Cluster, Version, Node Type, Number of Nodes, Database Engine")
    redshift_details("itx-acm")

