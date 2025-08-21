# This is a modified version that was used in class.
import boto3
import awswrangler as wr
from urllib.parse import unquote_plus
from pandas.errors import EmptyDataError
import time
import datetime

# Set AWS region for Wrangler
wr.config.region = "ap-southeast-1"

# SNS topic ARN (replace with yours)
SNS_TOPIC_ARN = "arn:aws:sns:ap-southeast-1:123456789012:data-pipeline-alerts"
 # Replace INITIALS with yours
QUARANTINE_BUCKET = "dataeng-landing-zone-INITIALS"

sns = boto3.client("sns")
s3 = boto3.client("s3")

def send_alert(subject, message):
    print(f"[ALERT] Sending SNS notification: {subject}")
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message
    )

def quarantine_file(bucket, key, reason):
    quarantine_key = f"quarantine/{key}"
    print(f"[WARNING] Moving file to quarantine: s3://{bucket}/{quarantine_key}")
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=quarantine_key
    )
    s3.delete_object(Bucket=bucket, Key=key)

    send_alert(
        subject=f"Quarantined File: {key}",
        message=f"""The file s3://{bucket}/{key} was quarantined at {datetime.datetime.utcnow()} UTC
Reason: {reason}
Moved to: s3://{bucket}/{quarantine_key}"""
    )

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])

    key_list = key.split("/")
    if len(key_list) < 3:
        quarantine_file(bucket, key, "Invalid S3 key structure")
        return {"warning": "Invalid path structure"}

    db_name = key_list[-3]
    table_name = key_list[-2]

    input_path = f"s3://{bucket}/{key}"
    # Replace INITIALS with yours
    output_path = f"s3://dataeng-clean-zone-INITIALS/{db_name}/{table_name}"

    print(f"[INFO] Processing: {input_path}")
    print(f"[INFO] Output path: {output_path}")
    print(f"[INFO] DB: {db_name}, Table: {table_name}")

    # Attempt to read the CSV
    try:
        input_df = wr.s3.read_csv([input_path])
    except EmptyDataError:
        quarantine_file(bucket, key, "CSV is empty or malformed")
        return {"warning": "Empty or invalid CSV"}

    if input_df.empty:
        quarantine_file(bucket, key, "CSV has headers but no data rows")
        return {"warning": "CSV has no data rows"}

    print(f"[INFO] Loaded {len(input_df)} rows")

    # Ensure Glue DB exists
    try:
        current_databases = wr.catalog.databases()
        if db_name not in current_databases['Database'].values:
            print(f"[INFO] Creating Glue database: {db_name}")
            wr.catalog.create_database(db_name)
    except Exception as e:
        print(f"[ERROR] Database check failed: {e}")
        raise

    # Convert and write to clean zone
    try:
        result = wr.s3.to_parquet(
            df=input_df,
            path=output_path,
            dataset=True,
            database=db_name,
            table=table_name,
            mode="append"
        )
        print(f"[SUCCESS] {len(input_df)} rows written to {output_path}")
        return result
    except Exception as e:
        print(f"[ERROR] Write failed: {e}")
        quarantine_file(bucket, key, f"Failed during write: {e}")
        raise
