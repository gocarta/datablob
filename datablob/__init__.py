import boto3
import csv
import io
import json


class DataBlobClient:
    def __init__(self, bucket_name, bucket_path):
        self.bucket_name = bucket_name
        self.bucket_path = bucket_path.rstrip("/")

    def _get_unique_keys(self, rows):
        columns = set()
        for row in rows:
            columns.update(row.keys())
        return list(sorted(list(columns)))

    def upload_csv(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path + "/" + dataset_name + "/v" + dataset_version + "/data.csv"
        )
        boto3.client("s3").put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data,
        )

    def get_dataset_as_csv(self, name, version, remove_bom=True):
        key = self.bucket_path + "/" + name + "/v" + version + "/data.csv"
        response = boto3.client("s3").get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")
        if remove_bom:
            object_content = object_content.lstrip("\ufeff")
        return object_content

    def get_dataset_as_json(self, name, version):
        key = self.bucket_path + "/" + name + "/v" + version + "/data.json"
        response = boto3.client("s3").get_object(Bucket=self.bucket_name, Key=key)
        object_content = response["Body"].read().decode("utf-8")
        return json.loads(object_content)

    def upload_json(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.json"
        )
        boto3.client("s3").put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data if isinstance(data, str) else json.dumps(data),
        )

    def upload_metadata(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/meta.json"
        )
        boto3.client("s3").put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=data if isinstance(data, str) else json.dumps(data),
        )

    def convert_rows_to_csv(self, rows, fieldnames=None):
        f = io.StringIO()
        if fieldnames is None:
            fieldnames = sorted(list(rows[0].keys()))
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        f.seek(0)
        # read and make sure we don't have a BOM
        return f.read().lstrip("\ufeff")

    def update_dataset(self, name, version, data, column_names=None):
        columns = column_names if column_names else self._get_unique_keys(data)
        meta = {"name": name, "numColumns": len(columns), "numRows": len(data)}
        data_as_csv = self.convert_rows_to_csv(data, fieldnames=columns)
        self.upload_csv(name, version, data_as_csv)
        self.upload_json(name, version, data)
        self.upload_metadata(name, version, meta)
