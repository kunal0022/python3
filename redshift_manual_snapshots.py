# ****************************************************************************************************************************************************************** #
#
#   Automation: List Out Redshift Manual Snapshots
#
# ****************************************************************************************************************************************************************** #

import boto3
import logging
import csv
import configparser
from datetime import datetime
from botocore.exceptions import ClientError


now = datetime.now()
date_time = now.strftime("%m-%d-%Y")

# *****************************************************************************
#
#   Server 1068
#
# *****************************************************************************

# keys_config = "/backup/automation/keys/.config.ini"

# log_filename = "{}{}{}".format(
#     "/backup/automation/redshift-manual-snaps/logs/snaps-", date_time, ".log"
# )
# report_name = "/backup/automation/redshift-manual-snaps/output/redshift-manual-snapshots.csv"


# *****************************************************************************
#
#   Server 5078
#
# *****************************************************************************

keys_config = "/app/automation/keys/.config.ini"

log_filename = "{}{}{}".format(
    "/app/automation/redshift_manual_snapshots/logs/snaps_", date_time, ".log"
)
report_name = "/app/automation/redshift_manual_snapshots/output/redshift_manual_snapshots.csv"

# *****************************************************************************
#
#   Windows
#
# *****************************************************************************

# keys_config = "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/keys/.config.ini"


# log_filename = "{}{}{}".format(
#     "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/redshift/logs/snaps-", date_time, ".log"
# )
# report_name = "C:/Users/MHern324/OneDrive - JNJ/Documents/data/automation_mhern/redshift/output/redshift-manual-snapshots.csv"

# ****************************************************************************************************************************************************************** #


config = configparser.ConfigParser()
config.read(keys_config)
rds_snaps = []

logging.basicConfig(
    filename=log_filename,
    filemode="a",
    format="%(asctime)s - %(message)s",
    level=logging.INFO,
)

# ****************************************************************************************************************************************************************** #


def conv_MB_to_GB(input_megabyte):
    gigabyte = 1.0 / 1024
    convert_gb = gigabyte * input_megabyte
    return round(convert_gb,2)


# ****************************************************************************************************************************************************************** #


def rds_get_manual_snaps(p_acct_id):
    try:
        aws_access_key_id = config.get(p_acct_id, "aws_access_key_id")
        aws_secret_access_key = config.get(p_acct_id, "aws_secret_access_key")
        aws_region = config.get(p_acct_id, "region")
        cli = boto3.client(
            "redshift",
            region_name=aws_region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
    
        resp = cli.describe_clusters()
        if len(resp["Clusters"]) > 0:
            for c in resp["Clusters"]:
                resp2 = cli.describe_cluster_snapshots(
                    ClusterIdentifier=c["ClusterIdentifier"], SnapshotType="manual"
                )
                if len(resp2["Snapshots"]) > 0:
                    for snap_info in resp2["Snapshots"]:
                        snap_datetime = snap_info["SnapshotCreateTime"]
                        rds_snaps.append(
                            {
                                "Account": p_acct_id,
                                "ClusterId": c["ClusterIdentifier"],
                                "SnapshotID": snap_info["SnapshotIdentifier"],
                                "CreatedAt": snap_datetime.strftime(
                                    "%m/%d/%Y %H:%M:%S"
                                ),
                                "SizeGB": conv_MB_to_GB(
                                    snap_info["TotalBackupSizeInMegaBytes"]
                                ),
                                "Type": snap_info["SnapshotType"],
                                "Status": snap_info["Status"],
                            }
                        )                       
    except ClientError as e:
        logging.error(
            "boto3 client error in ec2_get_status function: " + e.__str__(),
            exc_info=True,
        )
    except Exception as e:
        logging.exception(
            "Unexpected exception in ec2_get_statusfunction: " + e.__str__(),
            exc_info=True,
        )
    except:
        return rds_snaps

    # ****************************************************************************************************************************************************************** #


def rds_manual_snaps_report():
    if len(rds_snaps) > 0:
        for item in rds_snaps:
            fields = [
                "Account",
                "ClusterId",
                "SnapshotID",
                "CreatedAt",
                "SizeGB",
                "Type",
                "Status",
            ]
            with open(report_name, "w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rds_snaps)
    else:
        logging.error("No manual snapshots were found.")


# ****************************************************************************************************************************************************************** #


def main():
    print("Starting")
    dict_accounts = {
        "itx-acm",
        "itx-ags",
        "itx-ahr",
        # "itx-ajm",
        "itx-ajm-ireland",
        "itx-amt",
        "itx-anr",
        "itx-axy",
        "itx-bbi",
        "itx-bhw",
        "itx-bij",
        "itx-bnt",
        "itx-bpf",
        "itx-bsj",
        "itx-bxc",
        "itx-byr",
    }
    logging.info("Execution started at " + now.strftime("%m-%d-%Y, %H:%M:%S"))
    for key in dict_accounts:
        rds_get_manual_snaps(key)

    rds_manual_snaps_report()
    logging.info("Execution finished at " + now.strftime("%m-%d-%Y, %H:%M:%S"))
    print("Done")


# ****************************************************************************************************************************************************************** #

if __name__ == "__main__":
    main()
