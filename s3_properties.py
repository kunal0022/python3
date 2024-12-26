# ****************************************************************************************************************************************************************** #     
#
#   Automation S3: Find the versioning enabled buckets and status of lifecycle rules. https://jira.jnj.com/browse/JFMH-1187
#
# ****************************************************************************************************************************************************************** #     

import boto3
import botocore
import inspect
import csv
import logging
import configparser
from datetime import datetime
from botocore.exceptions import ClientError

s3_bucket_properties = []
s3_tags = []
s3_lcp = []
s3_owner = []
s3_client = ""
total_buckets = 0
versioning_buckets = 0
polcy_buckets = 0
lifecycle_buckets = 0

now = datetime.now()
date_time = now.strftime("%m-%d-%Y")

# *****************************************************************************
#
#   Server 1068
#
# *****************************************************************************

# keys_config = "/backup/automation/keys/.config.ini"

# log_filename = "{}{}{}".format(
#    "/backup/automation/s3-properties/logs/s3-properties-", date_time, ".log"
# )
# report_name = "/backup/automation/s3-properties/output/s3-properties.csv"


# *****************************************************************************
#
#   Server 5078
#
# *****************************************************************************

keys_config = "/app/automation/keys/.config.ini"

log_filename = "{}{}{}".format(
   "/app/automation/s3_properties/logs/s3_properties_", date_time, ".log"
)
report_name = "/app/automation/s3_properties/output/s3_properties.csv"

# *****************************************************************************
#
#   Windows
#
# *****************************************************************************

# keys_config = "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/keys/.config.ini"

# log_filename = "{}{}{}".format(
#    "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/s3/logs/s3-properties-", date_time, ".log"
# )
# report_name = "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/s3/output/s3-properties.csv"

# ****************************************************************************************************************************************************************** #

config = configparser.ConfigParser()
config.read(keys_config)
logging.basicConfig(filename=log_filename, filemode='a', format='%(asctime)s - %(message)s', level=logging.INFO)

 # ****************************************************************************************************************************************************************** # 

def get_all_bucket_names(p_acct_id):
   global total_buckets
   global s3_client

   try:
      frame = inspect.currentframe()
      
      aws_access_key_id = config.get(p_acct_id, 'aws_access_key_id')
      aws_secret_access_key = config.get(p_acct_id, 'aws_secret_access_key')
      aws_region = config.get(p_acct_id, 'region')
   
      s3_client = boto3.client('s3', region_name=aws_region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
      response = s3_client.list_buckets()
      for bucket in response['Buckets']:
         total_buckets += 1
         s3_creationdate = bucket["CreationDate"]
         # s3_bucket_properties.append({
         #                            'Index': total_buckets,
         #                            'AWSAccount': p_acct_id,
         #                            'BucketName': bucket["Name"],
         #                            'Owner': ''.join(get_s3_project_owner(bucket["Name"])),
         #                            'CreationDate':  s3_creationdate.strftime("%m/%d/%Y %H:%M:%S"),
         #                            'VersioningEnabled': get_bucket_versioning(bucket["Name"], p_acct_id), 
         #                            'LyfeCycleRules': ''.join(get_bucket_lifecycle_configuration(bucket["Name"])),
         #                            'Tags':''.join(get_bucket_tags(bucket["Name"]))
         #                         })
         s3_bucket_properties.append({
                                    'AWSAccount': p_acct_id,
                                    'BucketName': bucket["Name"],
                                    'Owner': ''.join(get_s3_project_owner(bucket["Name"])),
                                    'CreationDate':  s3_creationdate.strftime("%m/%d/%Y %H:%M:%S"),
                                    'VersioningEnabled': get_bucket_versioning(bucket["Name"], p_acct_id), 
                                    'LyfeCycleRules': ''.join(get_bucket_lifecycle_configuration(bucket["Name"])),
                                    'Tags':''.join(get_bucket_tags(bucket["Name"]))
                                 })      
         s3_tags.clear()
         s3_lcp.clear()
         s3_owner.clear()
      
   except botocore.exceptions.ClientError as e:
      logging.error("Couldn't get buckets."  + e.__str__())
      logging.error(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      # print(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      pass
   except ClientError as e:
      logging.exception( "boto3 client error in get_all_bucket_names function: "  + e.__str__())
      # print(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      logging.exception(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      pass
   except Exception as e:
      logging.exception("Unexpected error in get_all_bucket_names function: " + e.__str__())
      logging.exception(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      # print(" Bucket " + bucket["Name"] + "  Account " + p_acct_id)
      logging.exception(frame.f_code.co_name + "Error in Bucket " + bucket['Name'] )
      
 # ****************************************************************************************************************************************************************** #   

def get_bucket_lifecycle_configuration(s3BucketName):
   global lifecycle_buckets
   try:
      frame = inspect.currentframe()
      result = s3_client.get_bucket_lifecycle_configuration(Bucket=s3BucketName)['Rules']
      # result = s3_client.get_bucket_lifecycle_configuration(Bucket=s3BucketName)
      lifecycle_buckets += 1
      for rule in result:
         s3_lcp.append(f'| {rule["ID"]}: {rule["Status"]}')
      return s3_lcp  

   except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
         s3_lcp.append('No Life Cycle Rule')
         return s3_lcp
   except botocore.exceptions.DataNotFoundError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + "function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('Botocore exception data not found')
      pass
   except botocore.exceptions.ValidationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('Botocore exception Validation Error')
      pass
   except botocore.exceptions.UnsupportedS3ConfigurationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('Botocore exception Unsupported S3 Configuration')
      pass
   except botocore.exceptions.UnknownCredentialError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('Botocore exception Unknown Credentials')
      pass
   except botocore.exceptions.ClientError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('Botocore exception Client Error')
      pass
   except Exception as e:
      logging.exception( "Unexpected error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      s3_lcp.append('General Pytthon Exception')
      pass

 # ****************************************************************************************************************************************************************** #     

def get_bucket_versioning(s3BucketName, p_acct_id):
   global versioning_buckets
   try:
      frame = inspect.currentframe()
      result = s3_client.get_bucket_versioning(Bucket=s3BucketName)['Status']   
      return result
   
   except botocore.exceptions.DataNotFoundError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + "function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception data not found'
      pass
   except botocore.exceptions.ValidationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Validation Error'
      pass
   except botocore.exceptions.UnsupportedS3ConfigurationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unsupported S3 Configuration'
      pass
   except botocore.exceptions.UnknownCredentialError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unknown Credentials'
      pass
   except botocore.exceptions.ClientError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Client Error'
      pass
   except Exception as e:
      return 'Disabled'
      
   
 # ****************************************************************************************************************************************************************** #     

def get_bucket_tags(s3BucketName):
   try:
      frame = inspect.currentframe()
      bucket_tags = s3_client.get_bucket_tagging(Bucket=s3BucketName)
      for tag in bucket_tags.get('TagSet', []):
         s3_tags.append(f'| {tag["Key"]}: {tag["Value"]}')
      return s3_tags

   except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchTagSet':
         s3_tags.append('No Tags') 
         return s3_tags
   except botocore.exceptions.DataNotFoundError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + "function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception data not found'
      pass
   except botocore.exceptions.ValidationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Validation Error'
      pass
   except botocore.exceptions.UnsupportedS3ConfigurationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unsupported S3 Configuration'
      pass
   except botocore.exceptions.UnknownCredentialError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unknown Credentials'
      pass
   except botocore.exceptions.ClientError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Client Error'
      pass
   except Exception as e:
      logging.exception( "Unexpected error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'General Python Exception'
      pass

 # ****************************************************************************************************************************************************************** #     

def get_s3_project_owner(s3BucketName):
   try:
      frame = inspect.currentframe()
      bucket_tags = s3_client.get_bucket_tagging(Bucket=s3BucketName)
      for tag in bucket_tags.get('TagSet', []):
         if ("Project" in tag["Key"] or "project" in tag["Key"]):
            s3_owner.append(f' {tag["Key"]}: {tag["Value"]}')
      return s3_owner

   except botocore.exceptions.ClientError as e:
      if e.response['Error']['Code'] == 'NoSuchTagSet':
         s3_owner.append('No Project Owner') 
         return s3_owner
   except botocore.exceptions.DataNotFoundError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + "function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception data not found'
      pass
   except botocore.exceptions.ValidationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Validation Error'
      pass
   except botocore.exceptions.UnsupportedS3ConfigurationError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unsupported S3 Configuration'
      pass
   except botocore.exceptions.UnknownCredentialError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Unknown Credentials'
      pass
   except botocore.exceptions.ClientError as e:
      logging.exception( "boto3 client error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'Botocore exception Client Error'
      pass
   except Exception as e:
      logging.exception( "Unexpected error in " + frame.f_code.co_name + " function: " + e.__str__())
      logging.exception(frame.f_code.co_name + "Error in Bucket " + s3BucketName )
      return 'General Python Exception'
      pass

 # ****************************************************************************************************************************************************************** #     

def s3_report():
   try:
      frame = inspect.currentframe()
      for item in s3_bucket_properties:
         fields = ['AWSAccount', 'BucketName', 'Owner', 'CreationDate', 'VersioningEnabled', 'LyfeCycleRules' , 'Tags'] 

         with open(report_name, 'w', newline='') as file: 
            writer = csv.DictWriter(file, fieldnames = fields)    
            writer.writeheader()
            writer.writerows(s3_bucket_properties)
   except Exception as e:
      logging.exception( "Unexpected error on the CSV fike creation " + frame.f_code.co_name + " Error: " + e.__str__())
# ****************************************************************************************************************************************************************** #     

def main():
   dict_accounts = {
                  'itx-acm',
                  'itx-ags',
                  'itx-ahr',
                  'itx-ajm',
                  'itx-amt',
                  'itx-anr',
                  'itx-axy',
                  'itx-bbi',
                  'itx-bhw',
                  'itx-bij',
                  # 'Itx-bjc',
                  'itx-bnt',
                  'itx-bpf',
                  'itx-bsj',
                  'itx-bxc',
                  'itx-byr'
                  }
   
   # dict_accounts = {
   #                'itx-axy'
   #                }
   
   print('Execution begins')
   logging.info("Execution started at " + now.strftime("%m-%d-%Y, %H:%M:%S"))
   for key in dict_accounts:
      get_all_bucket_names(key)

   s3_report()
   logging.info("Execution finished at " + now.strftime("%m-%d-%Y, %H:%M:%S"))
   print('Execution ends')
   
 # ****************************************************************************************************************************************************************** # 

if __name__ == "__main__":
   main() 
