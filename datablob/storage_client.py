import io
from typing import Protocol, Union
import boto3


class StorageClientProtocol(Protocol):
    def put_object(self, key: str, body: Union[str, bytes, io.BytesIO]) -> None: ...

    def get_object_bytes(self, key: str) -> bytes: ...

    def list_keys(self, prefix: str) -> list[str]: ...


class S3Client:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")

    def put_object(self, key: str, body: Union[str, bytes, io.BytesIO]) -> None:
        if isinstance(body, (io.BytesIO, io.StringIO)):
            data = body.getvalue()
        else:
            data = body
        self.client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
        )

    def get_object_bytes(self, key: str) -> bytes:
        response = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return response["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        if "Contents" in response:
            for obj in response["Contents"]:
                keys.append(obj["Key"])
        return keys


class GcsClient:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError(
                "google-cloud-storage is required to use GCS with datablob. "
                "Install it with 'pip install datablob[gcs]'"
            )
        self.client = storage.Client()

    def put_object(self, key: str, body: Union[str, bytes, io.BytesIO]) -> None:
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(key)
        if isinstance(body, (io.BytesIO, io.StringIO)):
            data = body.getvalue()
        else:
            data = body
        blob.upload_from_string(data)

    def get_object_bytes(self, key: str) -> bytes:
        bucket = self.client.bucket(self.bucket_name)
        blob = bucket.blob(key)
        return blob.download_as_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        keys = []
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        for blob in blobs:
            keys.append(blob.name)
        return keys
