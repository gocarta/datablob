# datablob
Client for Updating a Simple Data Warehouse on Blob Storage

## install
```sh
pip install datablob
```

## usage
More examples coming soon
```py
from datablob import DataBlobClient

client = DataBlobClient(bucket_name="example-test-bucket-123", bucket_path="prefix/to/dataportal")

client.update_dataset(name="fleet", version="2", data=rows)
```