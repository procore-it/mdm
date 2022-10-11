from __future__ import print_function
import boto3
import datetime
import json

# current-date = datetime.date.today()
def lambda_handler(event, context):
# print(event)
  current_date = str(datetime.date.today())
  client = boto3.client('glue')
  for record in event['Records']:
    print("client-created")
    payload = json.loads(record["body"])
    source_system_name = str(payload["source_system_name"])
    source_bucket = str(payload["source_bucket"])
    destination_bucket = str(payload["destination_bucket"])
    persona = str(payload["persona"])
    expiration_timestamp = str(payload["expiration_timestamp"])
    email = str(payload["email"])
    isencryption = str(payload["isencryption"])
    print("paylaod proceesed")
    
    JobNameValue = 'MDM_Mask_Enc_Data_Job'
    
    response = client.start_job_run(
      JobName = JobNameValue,
      Arguments={
          '--source_system_name': source_system_name,
          '--source_bucket': source_bucket,
          '--destination_bucket': destination_bucket,
          '--persona': persona,
          '--expiration_timestamp': expiration_timestamp,
          '--email': email,
          '--isencryption':isencryption
      },
      #AllocatedCapacity=2,
      Timeout=5,
      #MaxCapacity=3,
      #SecurityConfiguration='string',
      NotificationProperty={
          'NotifyDelayAfter': 5
      },
      WorkerType='Standard',
      NumberOfWorkers=5
    )
  
  return "pass"
