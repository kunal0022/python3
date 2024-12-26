import boto3
import configparser

account_id = 'itx-amt'
config = configparser.ConfigParser()
config.read('/app/automation/keys/.config.ini')
aws_access_key_id = config[account_id]['aws_access_key_id']
aws_secret_access_key = config[account_id]['aws_secret_access_key']
region = config[account_id]['region']

def delete_eni_by_description(description):
    ec2 = boto3.client('ec2', region_name=region, aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key)
    response = ec2.describe_network_interfaces(
        Filters=[
            {'Name': 'description', 'Values': [description]},
            {'Name': 'status', 'Values': ['available']}
        ]
    )
    deleted_count = 0

    if 'NetworkInterfaces' in response:
        for interface in response['NetworkInterfaces']:
            eni_id = interface['NetworkInterfaceId']
            eni_description = interface['Description']
            eni_status = interface['Status']
            print(f"Deleted the ENI ID: {eni_id}, Description: {eni_description}, Status: {eni_status}")
            ec2.delete_network_interface(NetworkInterfaceId=eni_id)
            deleted_count += 1
        if deleted_count == 0:
            print(f"No ENI found with the specified description = {description} And status = available.")
        print(f"Deleted {deleted_count} ENI(s) successfully.")
    else:
        print(f"No ENI found with the specified description {description} And status = available.")


if __name__ == "__main__":
    description_to_delete = "Attached to Glue using role: arn:aws:iam::402683911681:role/itx-amt-iconnect-glue-role"
    delete_eni_by_description(description_to_delete)
