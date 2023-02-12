"""import boto3
s3_client = boto3.client("s3")
s3 = boto3.resource("s3")


# response = s3_client.upload_file(file_name, bucket, object_name)
# response = s3_client.upload_file('cat_0.png', '7014gree-test-bucket-01', 'cat.png')

my_bucket = s3.Bucket('7014gree-test-bucket-01')

for file in my_bucket.objects.all():
    print(file.key)

s3_client.download_file('7014gree-test-bucket-01', 'cat.png', 'cat_1.png')
"""
import requests
# Change this with your URL
url = 'https://7014gree-test-bucket-01.s3.amazonaws.com/cat.png'

response = requests.get(url)
with open('cat2.png', 'wb') as f:
    f.write(response.content)