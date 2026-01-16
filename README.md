# datablob
Client for Updating a Simple Data Warehouse on Blob Storage

## design philosophy
- optimize for simplicity and user friendliness
- storage is cheap (compared to compute)
- pre-compute as much as possible
- should work out of the box
- advanced configuration should be opt-in
- explicit is better than implicit
- straightforwardness over magic

## install
```sh
pip install datablob
```

## supported formats
- csv
- geojson points
- json


## usage
More examples coming soon
```py
from datablob import DataBlobClient

client = DataBlobClient(bucket_name="example-test-bucket-123", bucket_path="prefix/to/dataportal")

client.update_dataset(name="fleet", version="2", data=rows)
```