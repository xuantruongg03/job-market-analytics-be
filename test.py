import boto3

B2_KEY_ID = ""
B2_APP_KEY = ""
B2_ENDPOINT = "" 

# Tạo client với thông tin B2
s3 = boto3.client('s3',
    aws_access_key_id=B2_KEY_ID,
    aws_secret_access_key=B2_APP_KEY,
    endpoint_url=B2_ENDPOINT)

# Thử list file trong bucket
response = s3.list_objects_v2(Bucket="big-data", Prefix="task2/")
for obj in response.get("Contents", []):
    print(obj["Key"])
