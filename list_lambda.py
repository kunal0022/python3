import boto3
import os
from datetime import datetime
import configparser


config = configparser.ConfigParser()
config.read('/app/automation/keys/.config.ini')
# Load configuration from file

print(f"account, Lambda Function Name, Ignore,  Lambda Function ARN, Runtime")

def list_lambda_functions(aws_access_key_id, aws_secret_access_key):
    # Create a Boto3 client for Lambda
    client = boto3.client('lambda',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)

    # Initialize the pagination token
    next_marker = None

    # List to hold all Lambda functions
    lambda_functions = []

    while True:
        # List Lambda functions with pagination
        if next_marker:
            response = client.list_functions(Marker=next_marker)
        else:
            response = client.list_functions()

        # Add the functions to the list
        lambda_functions.extend(response['Functions'])

        # Check if there are more functions to list
        next_marker = response.get('NextMarker')
        if not next_marker:
            break

    return lambda_functions


def list_lambda_function(account):
    # Read AWS credentials from the configuration file
    aws_access_key_id = config.get(account, 'aws_access_key_id')
    aws_secret_access_key = config.get(account, 'aws_secret_access_key')
    serial_number = 0

    # Get the list of Lambda functions
    functions = list_lambda_functions(aws_access_key_id, aws_secret_access_key)

    # Print function details
    for function in functions:
        serial_number += 1
        ignore_list = "Y"
        x = function['FunctionName']
        if "custodian" not in x:
            ignore_list = "N"
        print(f"{account},{function['FunctionName']},{ignore_list},{function['FunctionArn']},{function['Runtime']}")

if __name__ == "__main__":

    list_lambda_function('itx-acm')
    list_lambda_function('itx-ags')
    list_lambda_function('itx-amt')
    list_lambda_function('itx-bxc')
    list_lambda_function('itx-bij')
    list_lambda_function('itx-dle')
    list_lambda_function('itx-bbi')
    list_lambda_function('itx-axy')
    list_lambda_function('itx-ajm')
    list_lambda_function('itx-anr')
    list_lambda_function('itx-bnt')
    list_lambda_function('itx-bpf')
    list_lambda_function('itx-bsj')


