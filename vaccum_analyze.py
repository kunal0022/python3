import psycopg2
import boto3
import time
import configparser
import pytz
from datetime import datetime, timedelta, timezone
import json
import sys
import configparser
config = configparser.ConfigParser()

region_name = 'us-east-1'
config.read('/app/automation/keys/.config.ini')
aws_access_key_id = config.get('ags', 'aws_access_key_id')
aws_secret_access_key = config.get('ags', 'aws_secret_access_key')
SecretId = sys.argv[1]
#SecretId = "#################################"

reboot_skip_list = ['itx-ajm-rs-itx-ajm-gcso-dev-rs-cl-01-gcsoqa', 'itx-ajm-rs-itx-ajm-gcso-dev-rs-cl-01-gcsopreprod', 'itx-amt-rs-itx-amt-scg-cia-rs-cl-01-crmpoc']


print(
    "\n*******************************************************************************************************************",flush=True)
print(f"The Process Started to Retrieve the SecretId '{SecretId}' at : ",
      datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
try:
    client = boto3.client('secretsmanager', region_name=region_name)
    client = boto3.client('secretsmanager', region_name=region_name, aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    response = client.get_secret_value(SecretId=SecretId)
    secret_values = json.loads(response['SecretString'])
    user_name = secret_values['user_name']
    #new_password = secret_values['password']
    new_password = "##########"
    host_name = secret_values['host']
    port_number = secret_values['port_number']
    region = secret_values['region']
    db_name = secret_values['db_name']
    cluster_name = secret_values['cluster_name']
    print(
        f"      user_name is : {user_name}\n      cluster_name is : {cluster_name}\n      host_name is : {host_name}\n      port_number is :{port_number}\n      DB Name is : {db_name}",flush=True)
    print(f"The Process Completed to Retrieve the SecretId '{SecretId}' at :",
          datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print(
        "*******************************************************************************************************************\n",flush=True)
except Exception as e:
    print(f"The Process Failed to Retrieve the SecretId '{SecretId}' at :",
          datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print(f"Error is : {e}",flush=True)
    print(
        "*******************************************************************************************************************\n",flush=True)
    sys.exit(1)

try:
    config.read('.config_key')
    #print(f"Secret Id is : {SecretId}")
    aws_access_key_id = config.get(SecretId, 'aws_access_key_id')
    aws_secret_access_key = config.get(SecretId, 'aws_secret_access_key')
except Exception as e:
    print(f"The Process Failed to Retrieve the Keys from .config_key at :", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print(f"\n")
    sys.exit(1)

try:
    print("*******************************************************************************************************************",flush=True)
    print(f"The Process Started to Create boto3 connection for Redshift at :",datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    redshift_client = boto3.client('redshift', region_name=region, aws_access_key_id=aws_access_key_id,
                                   aws_secret_access_key=aws_secret_access_key)
    session = boto3.Session(region_name=region, aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key)
    redshift_conn = session.client('redshift')
    print(f"The Process Succeeded to Create boto3 connection for Redshift using {aws_access_key_id} at :",datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print("*******************************************************************************************************************\n",flush=True)

#redshift_conn.autocommit = True
except Exception as e:
    print(f"The Process Failed to Create boto3 connection for Redshift using {aws_access_key_id} at :",datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print(f"Error is : {e}",flush=True)
    print("*******************************************************************************************************************\n",flush=True)
    exit(1)

#exit(0)
#exit(0)

if SecretId in reboot_skip_list:
    print("*******************************************************************************************************************",flush=True)
    print(f"Reboot Not needed for the Cluster : {cluster_name}",flush=True)
    print("Proceeding to next steps to check Cluster Status",flush=True)
    print("*******************************************************************************************************************\n",flush=True)

else:
    print("*******************************************************************************************************************",flush=True)
    print(f"Cluster is not in reboot Skip list. Hence Reboot Started for the Cluster : {cluster_name} at:", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    # reboot the cluster
    response_reboot = redshift_conn.reboot_cluster(
       ClusterIdentifier=cluster_name
    )
    #print(response_reboot)
    #exit(0)

    waiter = redshift_client.get_waiter('cluster_available')
    waiter.wait(
        ClusterIdentifier=cluster_name,
        WaiterConfig={
            'Delay': 30,
            'MaxAttempts': 60
        }
    )
    print(f"Reboot Completed for the Cluster : {cluster_name} at:", datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
    print("*******************************************************************************************************************\n",flush=True)

    print("*******************************************************************************************************************",flush=True)
    print("Process sleeps for 15 mins to make sure cluster is becoming to Available Status",flush=True)
    time.sleep(900)
    print("Sleep completed. Proceeding to next steps to check Cluster Status",flush=True)
    print("*******************************************************************************************************************\n",flush=True)


response = redshift_client.describe_clusters(ClusterIdentifier=cluster_name)
cluster = response['Clusters'][0]
cluster_status = cluster['ClusterAvailabilityStatus']
print("*******************************************************************************************************************",flush=True)
if cluster_status == "Available":
    print("Cluster is in Available status. Process moves to next step",flush=True)
    print("*******************************************************************************************************************\n",flush=True)

else:
    print(f"Cluster is Not in available Status. The status of cluster {cluster_name} is {cluster_status} Please wait for sometime and check. Script exists with failure.",flush=True)
    print("*******************************************************************************************************************",flush=True)
    exit(1)


counter = 0
try:
    print("*******************************************************************************************************************",flush=True)
    print(f"Connecting to DB with the above details",flush=True)
    conn = psycopg2.connect(
        host=host_name,
        port=port_number,
        database=db_name,
        user=user_name,
        password=new_password
    )
    cur = conn.cursor()
    print(f"Connection successful",flush=True)
    print("*******************************************************************************************************************\n",flush=True)
except Exception as e:
    print(f"Error connecting to DB with above details. Error is {e}",flush=True)
    exit(1)
    print("*******************************************************************************************************************\n",flush=True)

def sql_execution(sql_query):
    try:
        tz = pytz.timezone('US/Eastern')
        time_now = datetime.now(tz)
        print(
            "*******************************************************************************************************************",flush=True)
        print(f"{sql_query}  Query Started  at",
              datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
        #cur.execute('set autocommit=on;')
        cur.execute(sql_query)
        #cur.execute('set Autocommit=OFF;')
        print(f"{sql_query} Query executed Successfully at ",
              datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
        print(
            "*******************************************************************************************************************\n",flush=True)
    except Exception as e:
        print(f"Some error in {sql_query}: {e}")
        print(f"{sql_query} Query Failed at ",
              datetime.now(pytz.timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S %Z"),flush=True)
        print(
            "*******************************************************************************************************************\n",flush=True)
        exit(1)
    while conn.poll() != psycopg2.extensions.POLL_OK:
        continue

# sql_execution("END; set autocommit=on; VACUUM; COMMIT; ")
# sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
# cur.close()
# conn.close()

# if SecretId == '###########################':
#     sql_execution("END; set wlm_query_slot_count to 5; set autocommit=on; VACUUM; COMMIT; ")
#     sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
#     cur.close()
#     conn.close()
# else:
#     sql_execution("END; set autocommit=on; VACUUM; COMMIT; ")
#     sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
#     cur.close()
#     conn.close()


if SecretId == 'itx-amt-rs-itx-amt-scg-cia-rs-cl-01-crmpoc':
    sql_execution("END; set wlm_query_slot_count to 5; set autocommit=on; VACUUM; COMMIT; ")
    sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
    cur.close()
    conn.close()
elif SecretId == 'itx-amt-rs-itx-amt-ce-odp-rs-prod-cl-02':
    sql_execution("END; set wlm_query_slot_count to 5; set autocommit=on; VACUUM; COMMIT; ")
    sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
    cur.close()
    conn.close()
elif SecretId == 'itx-amt-rs-itx-amt-ce-odp-rs-uat-cl-02':
    sql_execution("END; set wlm_query_slot_count to 25; set autocommit=on; VACUUM; COMMIT; ")
    sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
    cur.close()
    conn.close()
else:
    sql_execution("END; set autocommit=on; VACUUM; COMMIT; ")
    sql_execution("END; set autocommit=on; Analyze; COMMIT; ")
    cur.close()
    conn.close()
