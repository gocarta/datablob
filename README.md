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
- xlsx (Microsoft Excel)

## basic usage
```py
from datablob import DataBlobClient

client = DataBlobClient(bucket_name="example-test-bucket-123", bucket_path="prefix/to/dataportal")

client.update_dataset(name="fleet", version="2", data=rows, xlsx=True)
# automatically creates the following files
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/meta.json
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.csv
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.json
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.jsonl
# s3://example-test-bucket-123/prefix/to/dataportal/fleet/v2/data.xlsx
```

## examples
- [Clever Vehicle Locations](https://github.com/gocarta/dataops-clever-vehicle-locations)
- [Simple Bus Routes](https://github.com/gocarta/dataops-simple-bus-routes)
- [Simple Bus Stops](https://github.com/gocarta/dataops-simple-bus-stops)
