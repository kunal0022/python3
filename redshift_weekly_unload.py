import logging
import boto3
import base64
from botocore.exceptions import ClientError
import json
import zipfile
import boto3
import json
import psycopg2
import os
import sys
import datetime
from datetime import date, datetime, timedelta

import configparser
config = configparser.ConfigParser()
config.read('/backup/automation/keys/.config.ini')
account = "ags"
aws_access_key_id = config.get(account, 'aws_access_key_id')
aws_secret_access_key = config.get(account, 'aws_secret_access_key')
logging.basicConfig(level=logging.INFO)
logging.info('This will get logged')
def get_secret(secret=None):
    secret_name = f"arn:aws:secretsmanager:us-east-1:244724910108:secret:{secret}"
    region_name = "us-east-1"

    # Create a Secrets Manager client

# Changed by Sekar
    client = boto3.client("secretsmanager", region_name="us-east-1",aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:

        print("before get secret value",flush=True)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret

    except ClientError as e:

        if e.response['Error']['Code'] == 'DecryptionFailureException':

            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':

            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e

        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e

        elif e.response['Error']['Code'] == 'InvalidRequestException':

            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.

            raise e

            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.


def redshift_exec(dbname: str = None, host: str = None, port: str = None, user: str = None, password: str = None, part1: str = None, part2: str = None, iamrole: str = None, s3name: str = None):
    con = psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
    date = datetime.today().strftime('%Y-%m-%d')
    startdate = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
    enddate = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=0)).strftime('%Y-%m-%d')


    query1 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name, 
        p.usesysid as userid, p.usename as username,v.last_access from pg_user p
        LEFT JOIN
        (SELECT username,max(recordtime) last_access from STL_CONNECTION_LOG where event=''authenticated'' group by username) 
        v ON p.usename=v.username')
        to 's3://{5}/AWS-Account-Metrics-New/User_History/'
        iam_role '{4}'
        partition by (account_name,cluster_name,data_export_date) include
        CSV DELIMITER AS ','
        PARALLEL OFF HEADER
        ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query2 = """unload ('SELECT ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name, 
        schema as schema_name,"table" as table_name, database, size as used_mb FROM svv_table_info 
        order by size desc')
        to 's3://{5}/AWS-Account-Metrics-New/Table_Size/'
        iam_role '{4}'
        partition by (account_name,cluster_name,data_export_date,database,schema_name) include
        CSV DELIMITER AS ','
        PARALLEL OFF
        HEADER ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)


    query3 = """unload('SELECT ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name, ''{3}'' as database,
                xx.schema as schema_name,nvl(round(yy.quota/1024,2),0) "allocated_quota_in_GB",round(xx.SizeInMB/1024,2) "disk_usage_in_GB", nvl(yy.disk_usage_pct,0) "usage_percentage" from
                ( select distinct schema, nvl(sum(mbytes),0) as SizeInMB from (   select trim(pgdb.datname) as Database, trim(pgn.nspname) as Schema,    trim(a.name) as Table, b.mbytes, a.rows     from pg_namespace as pgn      left join pg_class as pgc on pgn.oid = pgc.relnamespace     left join (select tbl, count(*) as mbytes from stv_blocklist group by tbl) b on pgc.oid=b.tbl     left join (select db_id, id, name, sum(rows) as rows from stv_tbl_perm a group by db_id, id, name ) as a on a.id = pgc.oid     left join pg_database as pgdb on pgdb.oid = a.db_id order by a.db_id, a.name ) group by schema order by schema ) as xx left outer join
                ( select SCHEMA_NAME , QUOTA, disk_usage, disk_usage_pct from svv_schema_quota_state ) as yy on  xx.schema = yy.SCHEMA_NAME
                where xx.schema not in (''pg_catalog'',''pg_internal'',''pg_toast'',''pg_automv'',''vpcx_schema'') and xx.schema not like ''pg_temp%'' and xx.schema not like ''%spectrum%''
                order by xx.schema')
           to 's3://{5}/AWS-Account-Metrics-New/Schema_Size/'
           iam_role '{4}'
           partition by (account_name,cluster_name,data_export_date,database)
           include CSV HEADER parallel off
           ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query4 = """unload ('SELECT ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,
            database,
            schema AS schemaname,
            table_id,
            "table" AS tablename,
            NVL(s.num_qs,0) num_qs,
            s.userid,
            s.usename,
            last_access,
            size as used_mb,
            week_ending
            FROM svv_table_info t
            LEFT JOIN (
            SELECT
            s.tbl, s.perm_table_name,
            COUNT(DISTINCT s.query) num_qs, s.userid, p.usename, MAX(s.endtime) last_access, date_trunc(''week'', s.starttime) as week_ending
            FROM
            stl_scan s join pg_user p on s.userid=p.usesysid
            WHERE
            s.userid > 1
            AND s.perm_table_name NOT IN (''Internal Worktable'',''S3'')
            GROUP BY
            s.tbl, s.perm_table_name,s.userid,p.usename,date_trunc(''week'', s.starttime)) s ON s.tbl = t.table_id
            AND t."schema" NOT IN (''pg_internal'')
            ORDER BY 7 desc')
            to 's3://{5}/AWS-Account-Metrics-New/Table_Query_Count/'
            iam_role '{4}'
            partition by (account_name,cluster_name,data_export_date,database,schemaname) include
            CSV HEADER parallel off
            ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query5 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, 
        ''{2}'' as cluster_name, ''{3}'' as database, s.userid, p.usename, trim(''S3 Scan'' from s.external_table_name) as spectrum_table_name , 
        count(v.querytxt) from (SELECT DISTINCT userid,query,external_table_name from  SVL_S3QUERY_SUMMARY) s
        LEFT JOIN  STL_QUERY v ON s.query=v.query
        LEFT JOIN  pg_user p on s.userid=p.usesysid
        GROUP BY s.userid, p.usename, s.external_table_name
        order by 4 desc')
        to 's3://{5}/AWS-Account-Metrics-New/Spectrum_Query_Count/'
        iam_role '{4}'
        partition by (account_name,cluster_name,data_export_date,database) include
        CSV HEADER parallel off
        ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)


    query6 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,
        trim(pu.usename) as username, coalesce(trim(sti.schema),''NA'') as schemaname, ''{5}'' as databasename, 
        coalesce(trim(sti.table), perm_table_name, ''NA'') as tablename, sq.query as query_id, 
        trim(sq.querytxt) as text, rows,sq.starttime,
        sq.endtime from (select userid, query, tbl, perm_table_name, min(starttime) starttime, sum(rows) "rows"
        from stl_scan where userid > 1 and step=0
        and perm_table_name not like ''stl_%''
        and perm_table_name not like ''volt_tt_%''
        and perm_table_name <> ''Runtime Filter''
        and perm_table_name <> ''Internal Worktable''
        group by userid, query, tbl,  perm_table_name) ss
        left join SVV_TABLE_INFO sti on sti.table_id = ss.tbl
        left join (Select usename,usesysid from  pg_user) pu on pu.usesysid = ss.userid
        inner join stl_query sq on ss.query = sq.query and ss.userid=sq.userid and ss.starttime between sq.starttime and sq.endtime
        where sq.starttime >= ''{3} 00:00:00'' and sq.endtime < ''{4} 23:59:59''
        and sq.querytxt not like ''padb_fetch%''
        and sq.querytxt not like ''CREATE TEMP TABLE %''
        and sq.querytxt not like ''Small %''
        and sq.querytxt not like ''small''
        and sq.querytxt not like ''fetch %''
        and sq.querytxt not like ''Undoing %''')
        to 's3://{7}/AWS-Account-Metrics-New/Queries_ran_by_users-new/'
        iam_role '{6}'
        partition by (account_name,cluster_name,data_export_date,databasename,schemaname) include 
        FORMAT PARQUET
        ALLOWOVERWRITE""".format(date, part1, part2, startdate, enddate, dbname, iamrole, s3name)


    query8 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,''{3}'' as databasename,
                 table_type,schemaname,table_count
                from
                (select ''local'' as table_type, schemaname,count(tablename) as table_count from pg_tables
                where schemaname not in (''pg_catalog'', ''pg_toast'',''information_schema'') and schemaname not like ''pg_temp%''
                group by schemaname
                order by schemaname)'
                )
                to 's3://{5}/AWS-Account-Metrics-New/Table_Count/'
                iam_role '{4}'
                partition by (account_name,cluster_name,data_export_date) include
                CSV DELIMITER AS ','
                PARALLEL OFF HEADER
                ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query9 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,''{3}'' as databasename,
                table_type,schemaname,table_count  from (select ''spectrum'' as table_type, schemaname as schemaname, count(tablename) as table_count from SVV_EXTERNAL_TABLES group by schemaname)')
                to 's3://{5}/AWS-Account-Metrics-New/Spectrum_Table_Count/'
                iam_role '{4}'
                partition by (account_name,cluster_name,data_export_date) include
                CSV DELIMITER AS ','
                PARALLEL OFF HEADER
                ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query10 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,''{3}'' as databasename,
                                table_type,schemaname,relname,col_count
                                from
                (select ''local'' as table_type, pgn.nspname as schemaname, pgc.relname as relname, attr.col_count as col_count
                from
                pg_class pgc
                JOIN pg_namespace pgn on pgc.relnamespace = pgn.oid::bigint
                JOIN pg_user pgu on pgu.usesysid =  pgc.relowner
                JOIN (select attrelid, count(1) col_count from pg_attribute where attnum > 0 group by attrelid) attr ON (attr.attrelid=pgc.oid::bigint)
                where pgn.nspname not in ( ''pg_catalog'',''pg_toast'',''information_schema'') and pgn.nspname not like ''%pg_temp%''
                order by pgn.nspname,pgc.relname)'
                )
                to 's3://{5}/AWS-Account-Metrics-New/Table_Column_Count/'
                iam_role '{4}'
                partition by (account_name,cluster_name,data_export_date) include
                CSV DELIMITER AS ','
                PARALLEL OFF HEADER
                ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)

    query11 = """unload ('select ''{1}'' as account_name, CURRENT_DATE as data_export_date, ''{2}'' as cluster_name,''{3}'' as databasename,
                         table_type,schemaname,tablename,col_count
                                                 from
                                                (select ''spectrum'' as table_type, schemaname, tablename, count(columnname) as col_count from svv_external_columns
                                                 group by schemaname, tablename
                                                 order by schemaname, tablename)'
                                            )
                to 's3://{5}/AWS-Account-Metrics-New/Spectrum_Table_Column_Count/'
                iam_role '{4}'
                partition by (account_name,cluster_name,data_export_date) include
                CSV DELIMITER AS ','
                PARALLEL OFF HEADER
                ALLOWOVERWRITE""".format(date, part1, part2, dbname, iamrole, s3name)



    cur = con.cursor()
    print(f'Starting data export for ' + part2 + ' at ' + datetime.today().strftime('%Y-%m-%d %H:%M:%S'),flush=True)

    value=[]
    #value.append(cur.execute(query0))
    #print(cur.execute(query0))
    cur.execute(query1)
    cur.execute(query2)
    cur.execute(query3)
    cur.execute(query4)
    cur.execute(query5)
    cur.execute(query6)
    cur.execute(query8)
    cur.execute(query9)
    cur.execute(query10)
    cur.execute(query11)
    # Close the cursor and the connection
    print(f'Data export completed for ' + part2 + ' at ' + datetime.today().strftime('%Y-%m-%d %H:%M:%S'),flush=True)
    cur.close()
    con.close()

# get_secret()

def main():


      cluster_lst = {"secret-ags-rs-itx-ags-prd-rs-cl-01":["cdeprddb"],
	         "secret-ags-rs-itx-ags-aw-rs-cl-01":["awprdrsdb"],
		     "secret-ags-rs-itx-ags-gcso-rs-cl-01":["cdescr"],
		     #"secret-ags-rs-itx-ags-anr-prd-rs-cl-01":["anrprdrsdb"],
		     "secret-acm-rs-cdedev1":["cdepoc"],
		     "secret-acm-rs-itx-acm-gcso-rs-cl-01": ["cdescr"],
		     #"cdedevemea":["cdedevemea"],
		     #"secret-bij-rs-itx-bij-ca-dev-rs-cl-01":["cadedb"],
		     #"secret-bij-rs-itx-bij-ca-prod-rs-cl-01":["caproddb"],
		     #"secret-ahr-rs-cerebroprod-dc": ["cerebroprod","proddb2"],
		     #"secret-ahr-rs-devcluster1": ["gpharmadb","test","selfserve"],
		     #"secret-ahr-rs-devcluster1": ["selfserve"],
		     #"secret-ahr-rs-jppuecspacred01": ["jppuecspadred01","phase2_dev","bridge_qa","ssa_qa","provider_dev","bridge_dev","devdb2","qadb2","ssa_dev","jaac_db_dev","selfserve"],
		     #"secret-ahr-rs-jppuecspacred01": ["jppuecspadred01","ssa_qa","bridge_dev","devdb2","ssa_dev","selfserve"],
		     #"secret-ahr-rs-jppuecspacred01-dc": ["jppuecspadred01","proddb1"],
		     #"secret-ahr-rs-prodcluster3": ["proddb3","ssa_bridge"],
		     "secret-ajm-rs-itx-ajm-gcso-dev-rs-cl-01": ["gcsodev", "gcsoqa","gcsopreprod"],
		     "secret-ajm-rs-itx-ajm-gcso-prod-rs-cl-01": ["gcsoprod"],
		     "secret-amt-rs-itx-amt-scg-cia-rs-cl-01": ["scgciadb","crmpoc"],
		     "secret-axy-rs-itx-axy-tempo-dev-rs-cl-01":["tempodev"],
		     "secret-axy-rs-itx-axy-tempo-prd-rs-cl-01":["tempoprddb"],
		     "secret-bnt-rs-itx-bnt-aw-dev-rs-cl-01":["awdev"],
		     "secret-bpf-rs-itx-bpf-patiente2e-prd-rs-cl-01": ["pe2eprddb"],
		     "secret-bpf-rs-itx-bpf-patiente2e-dev-rs-cl-01": ["pe2edevdb"],
		     "secret-bxc-rs-itx-bcx-biw-dev-rs-cl-01": ["cadevdb"],
		     "secret-bxc-rs-itx-bxc-biw-prd-rs-cl-02": ["biwprddb"]
           	      }

      for key in cluster_lst.keys():
        db = cluster_lst[key]
        #cluster=key
        #print(cluster)
        #print (db)
        scret = {}
        scret = get_secret(secret=key)

        #print(type(scret))
        #print(scret)
        scret = json.loads(scret)
        #print(type(scret))
        #print(scret.get('username'))
        for database in db:


            #cluster = key
            #print(cluster)
            dbname = database
            print(database,flush=True)
            account = scret.get('account')
            host = scret.get('host')
            port = scret.get('port')
            user = scret.get('username')
            password = scret.get('password')
            iamrole = scret.get('iamrole')
            s3name = scret.get('s3name')
            cluster = scret.get('clustername')

            redshift_exec(dbname=dbname, host=host, port=port, user=user, password=password, part1=account, part2=cluster, iamrole=iamrole, s3name=s3name)

main()
