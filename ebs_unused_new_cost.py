import boto3
import configparser
import pytz
from datetime import datetime

config = configparser.ConfigParser()
config.read('/app/automation/keys/.config.ini')

print("account,Name,VolumeId,State,Type,Size in GB,Volume related to kubernets/pvc,Created_time,Cost in USD")

def ebs_encrypt(account_id):
    aws_access_key_id = config[account_id]['aws_access_key_id']
    aws_secret_access_key = config[account_id]['aws_secret_access_key']
    region = config[account_id]['region']
    ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key)
    response = ec2.describe_volumes()

    for volume in response['Volumes']:
        if volume['State'] == 'available':
            state = volume['State']
            size = volume['Size']
            vol_id = volume['VolumeId']
            type = volume['VolumeType']
            CreateTime = volume['CreateTime'].astimezone(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z")
            cost = size * 0.08
            # Check if any tag name contains "KUBERNETS"
            tag_names = [tag['Key'] for tag in volume.get('Tags', [])]
            contains_keyword = any(
                keyword in tag_name.lower() for tag_name in tag_names for keyword in ["pvc", "kubernets", "eks"])

            contains_kubernets_new = "Y" if contains_keyword else "N"

            if "Name" in tag_names:
                pvc_name = next((tag['Value'] for tag in volume['Tags'] if tag['Key'] == "Name"), "N/A")
            else:
                pvc_name = "N/A"

            print(f"{account_id},{pvc_name},{vol_id},{state},{type},{size},{contains_kubernets_new},{CreateTime},{cost:.2f}")

ebs_encrypt("itx-acm")


