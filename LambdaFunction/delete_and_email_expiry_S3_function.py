import sys
import logging
import psycopg2
from datetime import datetime ,date ,timedelta
import boto3
import json
import smtplib,ssl
from email.message import EmailMessage

s3_resource = boto3.resource('s3')

rds_host  = "mdm-gov-postgres.cp7yvhmy7iuu.us-west-2.rds.amazonaws.com"
name = "postgres"
password = "Password"
db_name = "mdm"
port = 5432

logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    s3_client = boto3.client('ses')
    body = """
    The s3 bucket is about to expire in 5 days please contact system admin for more actions
    
    Thanks
    MDM Governance Team
    """
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to mail server.")
    logger.error(e)
    sys.exit()

try:
    conn = psycopg2.connect(host=rds_host, user=name, password=password, dbname=db_name, connect_timeout=5)
    conn.autocommit=True
except psycopg2.OperationalError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS PostgreSQL instance succeeded")

def lambda_handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """
    item_count = 0
    with conn.cursor() as cursor:
        recipients=list()
        cursor.execute("select email,expiration_date,destination_bucket,request_id from mdm_request where is_disabled =0 and deletion_notice_sent =0 and expiration_date > CURRENT_DATE")
        for email_row in cursor:
            item_count += 1
            logger.info(email_row)
        
            ExpectedDate_email = email_row[1]
            CurrentDate_email = datetime.today().date() + timedelta(days=5)
            
            if ExpectedDate_email == CurrentDate_email:
                try:
                    s3_client.send_email(
                        Source = 'sahodar.peddireddy@procore.com ',
                        Destination = {
                            'ToAddresses':[
                                email_row[0]
                            ]
                        },
                        Message = {
                            'Subject':{
                                'Data': 'MDM Data Governance expiry notification',
                                'Charset': 'UTF-8'
                            },
                            'Body':{
                                'Text':{
                                    'Data':body,
                                    'Charset': 'UTF-8'
                                }
                            }
                        }
                    )
                    sql= "update mdm_request set deletion_notice_sent = 1 where destination_bucket = %s and request_id = %s"
                    print(sql)
                    cursor = conn.cursor()
                    cursor.execute(sql,(email_row[2],email_row[3]))
                except Exception as e:
                    print(e)
            else:
                print("The expiration date of the bucket is:",ExpectedDate_email)
#delete s3 bucket once after expiry 
        cursor.execute("select destination_bucket,expiration_date,request_id from mdm_request where is_disabled = 0 and expiration_date < CURRENT_DATE")
        for delete_row in cursor:
            item_count += 1
            logger.info(delete_row)
        
            ExpectedDate_delete = delete_row[1]
            CurrentDate_delete = datetime.today().date()
            strCurrDate_delete = CurrentDate_delete.strftime("%Y-%m-%d")
        
            path_parts=delete_row[0].replace("s3://","").split("/")
            bucket=path_parts.pop(0)
            key="/".join(path_parts)
            
            if ExpectedDate_delete < CurrentDate_delete:
                try:
                    bucketvalue = s3_resource.Bucket(bucket)
                    response = bucketvalue.object_versions.filter(Prefix=key).delete()
                    print("the response:%s",response)
                    if not response:
                        print("Bucket object %s for deletion not available ",key)
                    else:
                        if response[0]['ResponseMetadata']['HTTPStatusCode'] == 200:
                            sql= "update mdm_request set is_disabled = 1 , deleted_timestamp = %s where destination_bucket = %s and request_id = %"
                            print(sql)
                            cursor = conn.cursor()
                            cursor.execute(sql,(strCurrDate_delete,delete_row[0],delete_row[2]))
                except Exception as e:
                    print(e)
            else:
                print("The expiration date of the bucket is:",ExpectedDate_delete)
    cursor.close()
    return "pass"