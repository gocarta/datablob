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
- [geojson points](https://geojson.org/)
- json
- [json lines](https://jsonlines.org/)
- [parquet](https://parquet.apache.org/), including [geoparquet](https://geoparquet.org/)

## usage
More examples coming soon
```py
from datablob import DataBlobClient

client = DataBlobClient(bucket_name="example-test-bucket-123", bucket_path="prefix/to/dataportal")

client.update_dataset(name="fleet", version="2", data=rows)
# automatically creates the following files
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/meta.json
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.csv
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.json
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.jsonl
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.parquet
```
