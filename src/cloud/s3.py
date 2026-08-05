from pathlib import Path
import boto3
from config.settings import settings


client = boto3.client(
    "s3",
    region_name=settings.AWS_REGION
)


def upload_file(file_path: Path):

    s3_key = str(file_path).replace("\\", "/")

    # Remove somente o prefixo data/
    if s3_key.startswith("data/"):
        s3_key = s3_key.removeprefix("data/")
        # Se estiver em Python < 3.9 use:
        # s3_key = s3_key[len("data/"):]

    client.upload_file(
        Filename=str(file_path),
        Bucket=settings.AWS_BUCKET,
        Key=s3_key
    )

    print(f"Upload realizado: s3://{settings.AWS_BUCKET}/{s3_key}")