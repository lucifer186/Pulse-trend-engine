from minio import Minio
from dotenv import load_dotenv
import os

load_dotenv()  # reads your .env file

client = Minio(
    "localhost:9000",
    access_key=os.getenv("MINIO_ROOT_USER"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
    secure=False  # local HTTP, not HTTPS
)

buckets = ["pulse-bronze", "pulse-silver", "pulse-gold"]

for bucket in buckets:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"✓ Created bucket: {bucket}")
    else:
        print(f"  Bucket already exists: {bucket}")

print("\nAll buckets ready!")