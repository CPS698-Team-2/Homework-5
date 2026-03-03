import os
import s3fs
from dotenv import load_dotenv

load_dotenv()

def main():
    print("Starting synchronization...")

    # Connect to MinIO
    minio = s3fs.S3FileSystem(
        key=os.getenv("MINIO_ACCESS_KEY"),
        secret=os.getenv("MINIO_SECRET_KEY"),
        client_kwargs={"endpoint_url": os.getenv("MINIO_ENDPOINT")}
    )

    minio_bucket = os.getenv("MINIO_BUCKET")

    # Connect to AWS S3
    s3 = s3fs.S3FileSystem()
    s3_bucket = os.getenv("AWS_S3_BUCKET")

    if not s3_bucket:
        raise RuntimeError("AWS_S3_BUCKET not set in .env")

    # Get all files under data-lake/
    prefix = f"{minio_bucket}/data-lake/"
    files = minio.glob(prefix + "**")

    count = 0

    for f in files:
        if f.endswith("/"):
            continue

        relative_path = f.replace(f"{minio_bucket}/", "")
        destination = f"{s3_bucket}/{relative_path}"

        print("Copying:", relative_path)

        with minio.open(f, "rb") as source_file:
            with s3.open(destination, "wb") as dest_file:
                dest_file.write(source_file.read())

        count += 1

    print("Sync complete.")
    print("Total files copied:", count)

if __name__ == "__main__":
    main()