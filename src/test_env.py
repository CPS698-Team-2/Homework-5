from dotenv import load_dotenv
import os

load_dotenv()

print("BOOTSTRAP:", os.getenv("BOOTSTRAP_SERVERS"))
print("MINIO:", os.getenv("MINIO_ENDPOINT"))