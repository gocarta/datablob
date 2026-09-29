import pytest

from datablob import DataBlobClient
from datablob.storage_client import GcsClient, S3Client


def test_init_with_providers():
    # s3 (default)
    client = DataBlobClient(bucket_name="my-bucket", bucket_path="path/to/data")
    assert client.bucket_name == "my-bucket"
    assert client.bucket_path == "path/to/data"
    assert client.provider == "s3"
    assert isinstance(client.storage_client, S3Client)

    # s3:// prefix
    client = DataBlobClient(bucket_name="s3://my-bucket", bucket_path="path/to/data")
    assert client.bucket_name == "my-bucket"
    assert client.provider == "s3"
    assert isinstance(client.storage_client, S3Client)

    # gcs provider
    client = DataBlobClient(
        bucket_name="my-gcs-bucket", bucket_path="path/to/data", provider="gcs"
    )
    assert client.bucket_name == "my-gcs-bucket"
    assert client.provider == "gcs"
    assert isinstance(client.storage_client, GcsClient)

    # gs:// prefix
    client = DataBlobClient(
        bucket_name="gs://my-gcs-bucket", bucket_path="path/to/data"
    )
    assert client.bucket_name == "my-gcs-bucket"
    assert client.provider == "gcs"
    assert isinstance(client.storage_client, GcsClient)

    # Invalid provider
    with pytest.raises(ValueError, match="Unsupported provider 'azure'"):
        DataBlobClient(bucket_name="my-bucket", bucket_path="path", provider="azure")
