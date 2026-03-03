import os
from dotenv import load_dotenv
import s3fs

load_dotenv()

fs = s3fs.S3FileSystem(
    key=os.getenv("MINIO_ACCESS_KEY"),
    secret=os.getenv("MINIO_SECRET_KEY"),
    client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")}
)

bucket = os.getenv("MINIO_BUCKET")

test_path = f"{bucket}/test_folder/hello.txt"

with fs.open(test_path, "w") as f:
    f.write("MinIO connection successful!")

print("File uploaded successfully.")