import boto3
import configparser
import pytz
from datetime import datetime, timedelta, timezone

config = configparser.ConfigParser()
config.read('/app/automation/keys/.config.ini')
import re

# print(f"The Process Started at : ", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
print("account|instance_id|host name| project| volume_id| Encrypted| Location| Root Volume|Size (GB)| Type | CreateTime|Tags")


def ebs_encrypt(account_id):
    aws_access_key_id = config[account_id]['aws_access_key_id']
    aws_secret_access_key = config[account_id]['aws_secret_access_key']
    region = config[account_id]['region']
    my_list = []
    ec2_list = []
    # Create an EC2 client
    # ec2 = boto3.client('ec2', region_name=region, aws_access_key_id="############", aws_secret_access_key="################")
    ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key)
    response_all_ec2 = ec2.describe_instances()
    for reservation in response_all_ec2['Reservations']:
        for instance in reservation['Instances']:
            # print(instance['InstanceId'])
            ec2_list.append(instance['InstanceId'])
    # print(ec2_list)
    account = account_id
    # print("\n*******************************************************************")

    for instance_id in ec2_list:

        all_volumes = ec2.describe_volumes(Filters=[{'Name': 'attachment.instance-id', 'Values': [instance_id]}])
        # Get the root volume ID
        response = ec2.describe_instances(InstanceIds=[instance_id])
        try:
            tags = response['Reservations'][0]['Instances'][0]['Tags']
        except Exception:
            tags = []

        for tag in tags:
            # print(tag['Key'], tag['Value'])
            if "Hostname" in tag['Key']:
                # print(f"{tag['Key']} : {tag['Value']}")
                HOST = tag['Value']
            else:
                HOST = 'N/A'
            if "Project" in tag['Key']:
                # print(f"{tag['Key']} : {tag['Value']}")
                Project = tag['Value']
            else:
                Project = 'N/A'

            # if "Account" in tag['Key']:
            # print(f"{tag['Key']} : {tag['Value']}")
            # account = tag['Value']
        try:
            root_volume_id = response['Reservations'][0]['Instances'][0]['BlockDeviceMappings'][0]['Ebs']['VolumeId']
        except Exception:
            root_volume_id = "abc"
        # print(f"instance_id is :  {instance_id}\n")
        # print(response)

        # print(f"            Root Volume is :  {root_volume_id}\n")

        for volume in all_volumes['Volumes']:
            volume_id = volume['VolumeId']
            encrypted = volume['Encrypted']
            device = volume['Attachments'][0]['Device']
            Size = volume['Size']
            CreateTime = volume['CreateTime']
            Tags = volume['Tags']
            type = volume['VolumeType']

            if encrypted:
                var = "Y"
            else:
                var = "N"
                my_list.append(volume_id)
            if root_volume_id == volume_id:
                root = 'Y'
            else:
                if root_volume_id == "abc":
                    root = "Can't Get"
                else:
                    root = 'N'
            # print(f"            Volume {volume_id} is {var} encrypted. Account is {account} hostname is {HOST}. project is {Project}. Location is : {device}")
            # print(volume)
            try:
                tags_str = ' , '.join([f"{tag['Key']}={tag['Value']}" for tag in Tags])

                project_match = re.search(r"Project=([\w\s]+)", tags_str)
                if project_match:
                    project_new = project_match.group(1).strip()

                else:
                    project_new = "N/A"

                # print("Project:", project)

            except Exception:
                tags_str = Tags
                project_new = "N/A"

            print(
                f"{account}|{instance_id}|{HOST}|{project_new}|{volume_id}|{var}|{device}| {root}| {Size}| {type} | {CreateTime}| {tags_str}")
        # count = len(my_list)
        # print(f"\n            Volumes to be encrypted: {count}. List is --> {my_list}")

    # Stop the instance
    # ec2.sto_instances(InstanceIds=[instance_id])

    # Wait for the instance to stop
    # waiter = ec2.get_waiter('instance_stopped')
    # waiter.wait(InstanceIds=[instance_id])

    # print("\n*******************************************************************")


ebs_encrypt("itx-bsj")
ebs_encrypt("itx-byr")


# print(f"The Process Completed at : ", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
