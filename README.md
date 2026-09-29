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

To include Google Cloud Storage (GCS) support:
```sh
pip install datablob[gcs]
```

## supported formats
- csv
- [geojson (points and polygons)](https://geojson.org/)
- json
- [json lines](https://jsonlines.org/)
- [parquet](https://parquet.apache.org/), including [geoparquet](https://geoparquet.org/)
- [shapefile (points and polygons)](https://en.wikipedia.org/wiki/Shapefile)
- tsv
- xlsx (Microsoft Excel)

## basic usage
```py
from datablob import DataBlobClient

# AWS S3 (default)
client = DataBlobClient(
    bucket_name="example-test-bucket-123", bucket_path="prefix/to/dataportal"
)

# Google Cloud Storage (GCS)
client = DataBlobClient(
    bucket_name="example-gcs-bucket-123", bucket_path="prefix/to/dataportal", provider="gcs"
)
# or using gs:// prefix:
client = DataBlobClient(
    bucket_name="gs://example-gcs-bucket-123", bucket_path="prefix/to/dataportal"
)

rows = [
    {
        "name": "Random Building",
        "@lat": 35.XXXXXX,
        "@lon": -85.XXXXXX,
        "@geom": { "type": "Polygon", "coordinates": [...] } // geojson geometry
    }
]

client.update_dataset(
    name="parking_lots", # name of the dataset, shouldn't include spaces or special characters
    version="2", # version of the dataset. It's a string, so you can version as you like.
    data=rows, # list of dictionaries.  Each row is a dictionary.
    latitude_key="@lat", # name of the latitude column 
    longitude_key="@lon", # name of the longitude column
    polygon_key="@geom", # name of the column with a polygon geometry in it (if applicable)
    xlsx=True # set to True to include an Excel file in output
)
# automatically creates the following files
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/meta.json
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.csv
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.points.geojson
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.polygons.geojson
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.json
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.jsonl
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.parquet
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.points.shp.zip
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.polygons.shp.zip
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.tsv
# s3://example-test-bucket-123/prefix/to/dataportal/buildings/v2/data.xlsx
```

## advanced usage
```py
client.update_dataset(
    name="fleet",
    version="1",
    data=rows,
    column_names=["ID", "Make", "Model", "Year"] # specify column names and their order
    description="List of Vehicles in Fleet",
    xlsx=True,
    xlsx_data_types=["Number", "Text", "Text", "Number"] # specify format of cell values in columns
)
```

## examples
- [Clever Vehicle Locations](https://github.com/gocarta/dataops-clever-vehicle-locations)
- [OpenStreetMap Parking](https://github.com/gocarta/dataops-osm-parking)
- [Simple Bus Routes](https://github.com/gocarta/dataops-simple-bus-routes)
- [Simple Bus Stops](https://github.com/gocarta/dataops-simple-bus-stops)
- [TN NG911 Address Points](https://github.com/gocarta/dataops-tn-ng911-address-points)
