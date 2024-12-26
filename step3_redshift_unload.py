import psycopg2
from datetime import datetime
import sys

s3_main = "s3://itx-ags-admin/AWS-Account-Metrics-New/Table_Not_Accessed"
conn = psycopg2.connect(
    host='itx-ags-prd-rs-cl-01.cku868xglwj7.us-east-1.redshift.amazonaws.com',
    port='5439',
    database='cdeprddb',
    user='#############',
    password='#############'
)

def get_redshift_count():
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM aws_account_metrics.redshift_table_not_accessed_local_view;")
        count = cur.fetchone()[0]
        conn.commit()
        return count
    except psycopg2.Error as e:
        print("Error fetching count from Redshift:", e)
        sys.exit(1)

def generate_s3_path():
    current_date = datetime.now()
    s3_path = current_date.strftime("%b_%d_%Y_")
    return s3_path

def unload_data():
    count = get_redshift_count()
    if count is not None:
        try:
            if count > 0:
                print(f"Count of 'aws_account_metrics.redshift_table_not_accessed_local_view' is {count}."+ "\n")
                print(f"Unloading data to S3 {s3_main} .." + "\n")

                cur = conn.cursor()
                s3_path = generate_s3_path()
                # print(s3_path)
                unload_query = f"""
                UNLOAD ('SELECT * FROM aws_account_metrics.redshift_table_not_accessed_local_view') 
                TO '{s3_main}/dataset_{s3_path}' 
                IAM_ROLE 'arn:aws:iam::244724910108:role/RedshiftSpectrumRole' CSV DELIMITER AS ',' PARALLEL OFF HEADER ALLOWOVERWRITE;
                """
                cur.execute(unload_query)
                conn.commit()
                print("Unload complete." + "\n")
            else:
                print("Count of 'aws_account_metrics.redshift_table_not_accessed_local_view' is 0. No need to unload.")
        except psycopg2.Error as e:
            print("Error unloading data to S3:", e )
            print("\n")
            sys.exit(1)
        finally:
            conn.close()
    else:
        print("Error: Unable to fetch count from Redshift."+ "\n")
        sys.exit(1)

if __name__ == "__main__":
    unload_data()
