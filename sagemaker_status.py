import boto3
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read('/app/automation/keys/.config.ini')

print("Account, Region, Instance Name, Current Status, Instance Type, Creation Time, Last Modified Time (Started/Stopped), In Service/stopped days ago, Project")

def sagemaker_stats(account, region):
    # Initialize the SageMaker client
    aws_access_key_id = config.get(account, 'aws_access_key_id')
    aws_secret_access_key = config.get(account, 'aws_secret_access_key')
    sagemaker = boto3.client('sagemaker', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region)

    # List SageMaker notebook instances
    response = sagemaker.list_notebook_instances()

    for instance in response['NotebookInstances']:
        instance_name = instance['NotebookInstanceName']
        instance_status = instance['NotebookInstanceStatus']
        creation_time = instance['CreationTime']
        instance_type = instance['InstanceType']
        LastModifiedTime = instance['LastModifiedTime']
        current_time = datetime.now(creation_time.tzinfo)
        running_time = current_time - LastModifiedTime
        running_days = running_time.days
        if running_days == 0:
            running_days = "Today"
        
        # Retrieve the tags for the instance
        tags_response = sagemaker.list_tags(ResourceArn=instance['NotebookInstanceArn'])
        project_tag = next((tag['Value'] for tag in tags_response['Tags'] if tag['Key'] == 'Project'), 'N/A')
        
        print(f"{account}, {region}, {instance_name}, {instance_status}, {instance_type}, {creation_time}, {LastModifiedTime}, {running_days}, {project_tag}")

sagemaker_stats('itx-acm', 'us-east-1')
sagemaker_stats('itx-bnt', 'us-east-1')
sagemaker_stats('itx-bjc', 'us-east-1')
sagemaker_stats('itx-bxc', 'ca-central-1')
