import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import datetime
import boto3
import hashlib, uuid
import base64
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
from pyspark.sql.types import StructType,StructField, IntegerType, StringType, DateType, DoubleType, MapType
from pyspark.sql.functions import to_date
from awsglue.dynamicframe import DynamicFrame
import json
import copy
from awsglue.gluetypes import _create_dynamic_record, _revert_to_dict
import time
from datetime import datetime
from awsglueml.transforms import EntityDetector
from pyspark import SQLContext
from pyspark.sql.functions import explode


sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)
#sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

#define connection to jdbc postgres tables
connection_options_parent_table = {
    "url": "jdbc:postgresql://mdm-gov-postgres.cp7yvhmy7iuu.us-west-2.rds.amazonaws.com:5432/mdm",
    "dbtable": "mdm_request",
    "user": "postgres",
    "password": "Password",
    "customJdbcDriverS3Path": "s3://mdm-govern-data-test/sourcecode/postgresql-42.2.20.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver"}
connection_options_child_table = {
    "url": "jdbc:postgresql://mdm-gov-postgres.cp7yvhmy7iuu.us-west-2.rds.amazonaws.com:5432/mdm",
    "dbtable": "mdm_request_child",
    "user": "postgres",
    "password": "Password",
    "customJdbcDriverS3Path": "s3://mdm-govern-data-test/sourcecode/postgresql-42.2.20.jar",
    "customJdbcDriverClassName": "org.postgresql.Driver"}
    
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['source_system_name','source_bucket','destination_bucket','persona','expiration_timestamp', 'email','isencryption'])
#read the CSV file for masking or encryption
def read_file_from_source_path(source_file_path):
    #global AmazonS3_source_bucket_node
    AmazonS3_source_bucket_node = glueContext.create_dynamic_frame.from_options(
            format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
            connection_type="s3",
            format="csv",
            connection_options={
                "paths": [source_file_path]},
            transformation_ctx="AmazonS3_source_bucket_node",
        )
    return AmazonS3_source_bucket_node
        
#detecteing the PII entities from the source file   
def detect_pii_entities(AmazonS3_source_bucket_node):
     # Script generated for node Detect PII
    #global detected_df
    entity_detector = EntityDetector()
    detected_df = entity_detector.detect(
        AmazonS3_source_bucket_node,
        [
            "PERSON_NAME",
            "EMAIL",
            "CREDIT_CARD",
            "IP_ADDRESS",
            "MAC_ADDRESS",
            "PHONE_NUMBER",
            "USA_PASSPORT_NUMBER",
            "USA_SSN",
            "USA_ITIN",
            "BANK_ACCOUNT",
            "USA_DRIVING_LICENSE",
            "USA_HCPCS_CODE",
            "USA_NATIONAL_DRUG_CODE",
            "USA_NATIONAL_PROVIDER_IDENTIFIER",
            "USA_DEA_NUMBER",
            "USA_HEALTH_INSURANCE_CLAIM_NUMBER",
            "USA_MEDICARE_BENEFICIARY_IDENTIFIER",
        ],
        "DetectedEntities",
    )
    return detected_df
    
def set_pii_fileds_list(detected_df):
    #global pii_fileds_list_string
    #Detected PII entities is converted as string value to write in RDS
    entities_value = detected_df.toDF()
    detected_entities_df = entities_value.select(explode(entities_value.DetectedEntities))
    detected_entities_df.createOrReplaceTempView("table_df")
    query_latest_rec = """SELECT distinct key FROM table_df"""
    latest_rec = sqlContext.sql(query_latest_rec)
    
    global list_value
    list_value = latest_rec.rdd.flatMap(lambda x: x).collect()
    pii_fileds_list_string = ','.join(list_value)
    #pii_fileds_list_string =  "[" + pii_fileds_list_string + "]"
    print(list_value)
    return pii_fileds_list_string

#Mask the detected PII entities    
def mask_pii_data(AmazonS3_source_bucket_node,detected_df):
     
    def replace_cell(original_cell_value, sorted_reverse_start_end_tuples):
        if sorted_reverse_start_end_tuples:
            for entity in sorted_reverse_start_end_tuples:
                to_mask_value = original_cell_value[entity[0] : entity[1]]
                original_cell_value = original_cell_value.replace(
                    to_mask_value, "########"
                )
        return original_cell_value

    def row_pii(column_name, original_cell_value, detected_entities):
        if column_name in detected_entities.keys():
            entities = detected_entities[column_name]
            start_end_tuples = map(
                lambda entity: (entity["start"], entity["end"]), entities
            )
            sorted_reverse_start_end_tuples = sorted(
                start_end_tuples, key=lambda start_end: start_end[1], reverse=True
            )
            return replace_cell(original_cell_value, sorted_reverse_start_end_tuples)
        return original_cell_value

    global row_pii_udf
    row_pii_udf = udf(row_pii, StringType())
    
    def recur(df, remaining_keys):
        if len(remaining_keys) == 0:
            return df
        else:
            head = remaining_keys[0]
            tail = remaining_keys[1:]
            modified_df = df.withColumn(
                head, row_pii_udf(lit(head), head, "DetectedEntities")
            )
            return recur(modified_df, tail)
            
    keys = AmazonS3_source_bucket_node.toDF().columns
    updated_masked_df = recur(detected_df.toDF(), keys)
    updated_masked_df = updated_masked_df.drop("DetectedEntities")
    return updated_masked_df

#encrypt detected PII entities
def get_encrypted_entities():
    global encrypted_entities
    encrypted_entities = list_value

def get_region_name():
    global my_region
    my_session = boto3.session.Session()
    my_region = my_session.region_name

def encrypt_rows(r):
    
    print ("encrypt_rows", salted_string, encrypted_entities)
    try:
        for entity in encrypted_entities:
            salted_entity = r[entity] + salted_string
            hashkey = hashlib.sha256(salted_entity.encode()).hexdigest()
            r[entity] = hashkey
    except:
        print ("DEBUG:",sys.exc_info())
    return r
    
def set_salted_string():
    global salted_string
    salted_string=create_data_key()
    
def create_data_key():
    kms_client = boto3.client('kms', region_name='us-west-2')
    key_id = 'arn:aws:kms:us-west-2:785368447960:key/b3cd4ad6-4e95-495e-b534-1b0a9a3f1955'
    response = kms_client.generate_data_key(KeyId=key_id,KeySpec='AES_128')
    plaintext_key = response['CiphertextBlob']
    bytesvalue = base64.b64encode(plaintext_key)
    salted_string = bytesvalue.decode('ascii')
    print(salted_string)
    return salted_string
    
#Write masked/encrypted file to the destination bucket_name
def write_file_to_destination(updated_masked_df,file_name):
    DetectPII_node = DynamicFrame.fromDF(
            updated_masked_df, glueContext, "updated_masked_df"
        )
       
    # Script generated for node Amazon S3
    AmazonS3_dest_bucket_node = glueContext.write_dynamic_frame.from_options(
        frame=DetectPII_node,
        connection_type="s3",
        format="csv",
        connection_options={
            "path": dest_bucket_path,
            "partitionKeys": [],
        },
        transformation_ctx="AmazonS3_dest_bucket_node"+file_name,
    )

#writing record to RDS Parent_child database
def write_record_RDS_Parent_child(noOfSlash,object_path,file_name,pii_fileds_list_string,updated_masked_df_value):
    print("write a record to Database") 
    schema = StructType([ \
    StructField("system_name",StringType(),True), \
    StructField("source_bucket",StringType(),True), \
    StructField("destination_bucket",StringType(),True), \
    StructField("persona", StringType(), True), \
    StructField("expiration_timestamp", DateType(), True), \
    StructField("is_disabled", IntegerType(), True), \
    StructField("email", StringType(), True), \
    StructField("deletion_notice_sent", IntegerType(), True), \
    StructField("pii_columns_list",StringType(), True), \
    StructField("folder_file_name", StringType(), True) \
    ])
    expiration_timestamp = args['expiration_timestamp'] 
    values = [('Glue Job',args['source_bucket'],args['destination_bucket'],args['persona'],datetime.strptime(expiration_timestamp,'%Y-%m-%d'),0,args['email'],0,pii_fileds_list_string,file_name)]
    mdm_record_df = DynamicFrame.fromDF(spark.createDataFrame(data = values, schema = schema), glueContext, "mdm_record_df")
    glueContext.write_from_options(frame_or_dfc=mdm_record_df, connection_type="postgresql",connection_options=connection_options_parent_table)
    
    #Child table update with folder name and parent_table request id
    #read_updated_df = glueContext.create_dynamic_frame.from_options(connection_type="postgresql", connection_options=connection_options_parent_table)
    #df_map = Map.apply(frame = read_updated_df, f = updated_masked_df_value)
    #updated_df = read_updated_df.toDF()
    df = spark.read\
            .format('jdbc')\
            .option('url', 'jdbc:postgresql://mdm-gov-postgres.cp7yvhmy7iuu.us-west-2.rds.amazonaws.com:5432/mdm')\
            .option('driver', 'org.postgresql.Driver')\
            .option('dbtable','mdm_request')\
            .option('user','postgres')\
            .option('password','Password')\
            .load()
   
    df.createOrReplaceTempView("table_df")
    query_latest_rec = """SELECT request_id FROM table_df ORDER BY request_id DESC limit 1"""
    latest_rec = sqlContext.sql(query_latest_rec)
    print(latest_rec.show())
    #print("after latest_rec.show()")
    #df_requestid = latest_rec.toDF()
    list_value = latest_rec.rdd.flatMap(lambda x: x).collect()
    for request_parent_value in list_value:
        print("request_parent_value")
        list_requestid =request_parent_value
        print(object_path)
        print(source_path)
    if noOfSlash >1 and object_path != source_path:
        print("inside if")
        object_path_split = object_path.split("/")
        folder_name = object_path_split[-2]
        schema = StructType([ \
            StructField("request_parent_id", IntegerType() ,True), \
            StructField("folder_name",StringType(),True), \
             StructField("pii_columns_list",StringType(), True) \
          ])
        values = [(list_requestid, folder_name,pii_fileds_list_string)]
        mdm_record_folder_df = DynamicFrame.fromDF(spark.createDataFrame(data = values, schema = schema), glueContext, "mdm_record_df_folder")
        glueContext.write_from_options(frame_or_dfc=mdm_record_folder_df, connection_type="postgresql",connection_options=connection_options_child_table)
        
        
job.init('MDM_Mask_Enc_Data_Job', args)
# Script generated for node Amazon S3
source_path = args['source_bucket']
dest_bucket_path = args['destination_bucket']
is_encrypt= args['isencryption']
path_parts=source_path.replace("s3://","").split("/")
bucket_name=path_parts.pop(0)
key="/".join(path_parts)
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket(bucket_name)
file_name=""
for object in bucket.objects.filter(Prefix=key):
    pii_fileds_list_string=""
    if object.key.endswith(".csv"):
        object_path = "s3://"+bucket_name+"/"+object.key
        file_name = object.key
        noOfSlash = file_name.count("/")
        AmazonS3_source_bucket_node_value = read_file_from_source_path(object_path)
        detected_df_value = detect_pii_entities(AmazonS3_source_bucket_node_value)
        pii_fileds_list_string = set_pii_fileds_list(detected_df_value)
        if is_encrypt =="false":
            print("inside if of is_encrypt")
            updated_masked_df_value = mask_pii_data(AmazonS3_source_bucket_node_value,detected_df_value)
        else:
            AmazonS3_source_bucket_node_value_df = AmazonS3_source_bucket_node_value.toDF()
            group_df = DynamicFrame.fromDF(AmazonS3_source_bucket_node_value_df, glueContext, "group_df")
            get_region_name()
            set_salted_string()
            get_encrypted_entities()
            df_with_encrypted = Map.apply(frame = group_df, f = encrypt_rows)
            updated_masked_df_value = df_with_encrypted.toDF()
        write_file_to_destination(updated_masked_df_value,file_name)
        write_record_RDS_Parent_child(noOfSlash,object_path,file_name,pii_fileds_list_string,updated_masked_df_value)

job.commit()