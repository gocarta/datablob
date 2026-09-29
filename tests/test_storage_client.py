import io
import pytest
from unittest.mock import MagicMock, patch

from datablob.storage_client import GcsClient, S3Client


@patch("boto3.client")
def test_s3_client_put_object(mock_boto3_client):
    s3_client = S3Client("my-s3-bucket")

    s3_client.put_object("path/to/file.txt", "hello world")
    mock_boto3_client.return_value.put_object.assert_called_with(
        Bucket="my-s3-bucket",
        Key="path/to/file.txt",
        Body="hello world",
    )

    bytes_io = io.BytesIO(b"binary data")
    s3_client.put_object("path/to/binary.dat", bytes_io)
    mock_boto3_client.return_value.put_object.assert_called_with(
        Bucket="my-s3-bucket",
        Key="path/to/binary.dat",
        Body=b"binary data",
    )


@patch("boto3.client")
def test_s3_client_get_object(mock_boto3_client):
    mock_body = MagicMock()
    mock_body.read.return_value = b"sample content"
    mock_boto3_client.return_value.get_object.return_value = {"Body": mock_body}

    s3_client = S3Client("my-s3-bucket")
    content = s3_client.get_object_bytes("path/to/file.txt")

    assert content == b"sample content"
    mock_boto3_client.return_value.get_object.assert_called_once_with(
        Bucket="my-s3-bucket",
        Key="path/to/file.txt",
    )


@patch("boto3.client")
def test_s3_client_list_keys(mock_boto3):
    mock_boto3_client = mock_boto3.return_value
    mock_boto3_client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "prefix/ds1/v1/meta.json"},
            {"Key": "prefix/ds1/v1/data.csv"},
        ]
    }

    s3_client = S3Client("my-s3-bucket")
    keys = s3_client.list_keys("prefix")

    assert keys == ["prefix/ds1/v1/meta.json", "prefix/ds1/v1/data.csv"]
    mock_boto3_client.list_objects_v2.assert_called_once_with(
        Bucket="my-s3-bucket",
        Prefix="prefix",
    )


@patch.dict("sys.modules", {"google.cloud": None})
def test_gcs_client_missing_import():
    with pytest.raises(ImportError, match="google-cloud-storage is required"):
        GcsClient("my-gcs-bucket")


@patch("google.cloud.storage.Client")
def test_gcs_client_operations(mock_gcs_client_cls):
    mock_client_instance = mock_gcs_client_cls.return_value
    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_client_instance.bucket.return_value = mock_bucket
    mock_bucket.blob.return_value = mock_blob

    gcs_client = GcsClient("my-gcs-bucket")

    gcs_client.put_object("path/to/file.txt", "hello gcs")
    mock_client_instance.bucket.assert_called_with("my-gcs-bucket")
    mock_bucket.blob.assert_called_with("path/to/file.txt")
    mock_blob.upload_from_string.assert_called_with("hello gcs")

    bytes_io = io.BytesIO(b"gcs binary")
    gcs_client.put_object("path/to/binary.dat", bytes_io)
    mock_blob.upload_from_string.assert_called_with(b"gcs binary")

    mock_blob.download_as_bytes.return_value = b"retrieved gcs bytes"
    content = gcs_client.get_object_bytes("path/to/file.txt")
    assert content == b"retrieved gcs bytes"

    blob1 = MagicMock()
    blob1.name = "prefix/file1.csv"
    blob2 = MagicMock()
    blob2.name = "prefix/file2.csv"
    mock_client_instance.list_blobs.return_value = [blob1, blob2]

    keys = gcs_client.list_keys("prefix")
    assert keys == ["prefix/file1.csv", "prefix/file2.csv"]
    mock_client_instance.list_blobs.assert_called_once_with(
        "my-gcs-bucket", prefix="prefix"
    )
