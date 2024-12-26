import boto3
import configparser
import warnings

warnings.filterwarnings('ignore', category=FutureWarning, module='botocore.client')

config = configparser.ConfigParser()
#config.read('/speriya4/.config_key_s3')
config.read('/app/automation/keys/.config.ini')

def get_efs_details(account_id):
    def get_efs_file_systems(region, access_key, secret_key):
        client = boto3.client('efs', region_name=region, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        return client.describe_file_systems()['FileSystems']

    aws_access_key_id = config[account_id]['aws_access_key_id']
    aws_secret_access_key = config[account_id]['aws_secret_access_key']
    region = config[account_id]['region']

    for efs in get_efs_file_systems(region, aws_access_key_id, aws_secret_access_key):
        #print(efs)
        efs_id = efs['FileSystemId']
        try:
            efs_name = efs['Name']
        except:
            efs_name = 'name not available'
        efs_size_standard = efs['SizeInBytes']['ValueInStandard'] / 1024**3  # Convert bytes to GB
        efs_size_infrequent_access = efs['SizeInBytes']['ValueInIA'] / 1024 ** 3  # Convert bytes to GB
        efs_size_total = efs['SizeInBytes']['Value'] / 1024 ** 3  # Convert bytes to GB

        standard_cost = 0.30 * efs_size_standard
        infrequent_cost = 0.016 * efs_size_standard
        total_cost = standard_cost + infrequent_cost

        print(f"{account_id}, {efs_name}, {efs_id}, {efs_size_standard:.2f} GB, {efs_size_infrequent_access:.2f} GB, {efs_size_total:.2f} GB, {standard_cost:.2f}, {infrequent_cost:.2f}, {total_cost:.2f}")
        #print(efs)

if __name__ == "__main__":
    print("Account, EFS Name, FileSystemId, efs_size_standard (GB), efs_size_infrequent_access (GB), Size_total (GB), standard_cost, infrequent_cost, total_cost")
    get_efs_details("itx-acm")
    get_efs_details("itx-ags")
    get_efs_details("itx-ahr")
    get_efs_details("itx-ajm")
    get_efs_details("itx-amt")
    get_efs_details("itx-bpf")
    get_efs_details("itx-bxc")
    # get_efs_details("itx-bhw")
    get_efs_details("itx-bnt")
    get_efs_details("itx-anr")
    get_efs_details("itx-axy")
    get_efs_details("itx-bbi")
    get_efs_details("itx-bij")
    get_efs_details("itx-bsj")
    get_efs_details("itx-byr")
    get_efs_details("itx-dle")
