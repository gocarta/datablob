import csv
from datetime import datetime
import geopandas as gpd
import io
from json import dumps as json_dumps
from json import loads as json_loads
from openpyxl import Workbook
import os
import pandas as pd
from shapely.geometry import shape
import tempfile
import zipfile
from zoneinfo import ZoneInfo

from datablob.storage_client import (
    GcsClient as GcsClient,
    S3Client,
    StorageClientProtocol,
)

POSSIBLE_LATITUDE_KEYS = [
    "LATITUDE",
    "Latitude",
    "latitude",
    "LAT",
    "Lat",
    "lat",
    "@lat",
]

POSSIBLE_LONGITUDE_KEYS = [
    "LONGITUDE",
    "Longitude",
    "longitude",
    "LONG",
    "Long",
    "long",
    "LON",
    "Lon",
    "lon",
    "@lon",
]


class DataBlobClient:
    storage_client: StorageClientProtocol

    def __init__(
        self,
        bucket_name,
        bucket_path,
        timezones=None,
        provider="s3",
        storage_client: StorageClientProtocol | None = None,
    ):
        if bucket_name.startswith("gs://"):
            provider = "gcs"
            bucket_name = bucket_name[5:]
        elif bucket_name.startswith("s3://"):
            provider = "s3"
            bucket_name = bucket_name[5:]

        self.bucket_name = bucket_name
        self.bucket_path = bucket_path.rstrip("/")
        self.timezones = timezones or ["UTC"]
        self.provider = provider.lower()

        if storage_client is not None:
            self.storage_client = storage_client
        elif self.provider == "s3":
            self.storage_client = S3Client(self.bucket_name)
        elif self.provider == "gcs":
            self.storage_client = GcsClient(self.bucket_name)
        else:
            raise ValueError(
                f"Unsupported provider '{self.provider}'. Must be 's3' or 'gcs'."
            )

    def _get_unique_keys(self, rows):
        columns = set()
        for row in rows:
            columns.update(row.keys())
        return list(sorted(list(columns)))

    def _flatten_rows(self, rows):
        flat_rows = []
        for row in rows:
            flat_row = {}
            for key, value in row.items():
                if isinstance(value, dict):
                    flat_row[key] = json_dumps(value)
                else:
                    flat_row[key] = value
            flat_rows.append(flat_row)
        return flat_rows

    def get_filenames_by_dataset_and_version(self):
        results = {}
        keys = self.storage_client.list_keys(prefix=self.bucket_path)
        prefix_len = len(self.bucket_path) + 1 if self.bucket_path else 0
        for full_key in keys:
            key = full_key[prefix_len:] if prefix_len else full_key

            if len(key.split("/")) == 3:
                [dataset_id, version, filename] = key.split("/")
                if version.startswith("v"):
                    version = version[1:]
                    if filename:
                        if dataset_id not in results:
                            results[dataset_id] = {}
                        if version not in results[dataset_id]:
                            results[dataset_id][version] = []
                        results[dataset_id][version].append(filename)

        for dataset_id, subdict in results.items():
            versions = list(subdict.keys())
            for version in versions:
                files = subdict[version]
                if "meta.json" in files:
                    subdict[version] = sorted(subdict[version])
                else:
                    del subdict[version]
        return results

    def upload_csv(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path + "/" + dataset_name + "/v" + dataset_version + "/data.csv"
        )
        self.storage_client.put_object(key, data)

    def upload_tsv(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path + "/" + dataset_name + "/v" + dataset_version + "/data.tsv"
        )
        self.storage_client.put_object(key, data)

    def get_dataset_as_csv(self, name, version, remove_bom=True):
        key = self.bucket_path + "/" + name + "/v" + version + "/data.csv"
        object_content = self.storage_client.get_object_bytes(key).decode("utf-8")
        if remove_bom:
            object_content = object_content.lstrip("\ufeff")
        return object_content

    def get_dataset_as_tsv(self, name, version, remove_bom=True):
        key = self.bucket_path + "/" + name + "/v" + version + "/data.tsv"
        object_content = self.storage_client.get_object_bytes(key).decode("utf-8")
        if remove_bom:
            object_content = object_content.lstrip("\ufeff")
        return object_content

    def get_dataset_as_json(self, name, version):
        key = self.bucket_path + "/" + name + "/v" + version + "/data.json"
        object_content = self.storage_client.get_object_bytes(key).decode("utf-8")
        return json_loads(object_content)

    def get_dataset_metadata(self, name: str, version: str):
        key = self.bucket_path + "/" + name + "/v" + version + "/meta.json"
        object_content = self.storage_client.get_object_bytes(key).decode("utf-8")
        return json_loads(object_content)

    def upload_geojson_points(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.points.geojson"
        )
        self.storage_client.put_object(
            key,
            data if isinstance(data, str) else json_dumps(data),
        )

    def upload_geojson_polygons(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.polygons.geojson"
        )
        self.storage_client.put_object(
            key,
            data if isinstance(data, str) else json_dumps(data),
        )

    def upload_shapefile_points(self, dataset_name, dataset_version, blob):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.points.shp.zip"
        )
        self.storage_client.put_object(key, blob)

    def upload_shapefile_polygons(self, dataset_name, dataset_version, blob):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.polygons.shp.zip"
        )
        self.storage_client.put_object(key, blob)

    def upload_jsonl(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.jsonl"
        )

        results = ""
        for row in data:
            results += json_dumps(row) + "\n"

        self.storage_client.put_object(key, results)

    def upload_json(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.json"
        )
        self.storage_client.put_object(
            key,
            data if isinstance(data, str) else json_dumps(data),
        )

    def upload_parquet(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.parquet"
        )
        self.storage_client.put_object(key, data)

    def upload_xlsx(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/data.xlsx"
        )
        self.storage_client.put_object(key, data)

    def upload_metadata(self, dataset_name, dataset_version, data):
        key = (
            self.bucket_path
            + "/"
            + dataset_name
            + "/v"
            + dataset_version
            + "/meta.json"
        )
        self.storage_client.put_object(
            key,
            data if isinstance(data, str) else json_dumps(data, indent=4),
        )

    def convert_gdf_to_shapefile(self, gdf):
        buf = io.BytesIO()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "data.shp")
            gdf.to_file(path, driver="ESRI Shapefile")

            # copy temp files into in-memory zip file
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for filename in os.listdir(tmpdir):
                    zf.write(os.path.join(tmpdir, filename), filename)

        buf.seek(0)
        return buf.getvalue()

    def _convert_row_to_xlsx_data_types(self, row, xlsx_data_types):
        result = []
        for icol, value in enumerate(row):
            data_type = xlsx_data_types[icol]
            if value is None:
                value = ""
            elif data_type == "Number":
                if value != "":
                    value = float(value)
            elif data_type == "Text":
                value = str(value)
            else:
                print("[datablob] warning: unsupported xlsx data type " + data_type)
                value = str(value)
            result.append(value)
        return result

    def convert_to_xlsx(self, meta, data, columns, xlsx_data_types=None):
        wb = Workbook()
        ws_overview = wb.active
        ws_overview.title = "Overview"
        ws_overview.append(["name", meta["name"]])
        for tz, value in meta["lastUpdated"].items():
            ws_overview.append(["last updated (in " + tz + ")", value])
        ws_overview.append(["description", meta["description"]])
        ws_overview.append(["number of columns", meta["numColumns"]])
        ws_overview.append(["number of rows", meta["numRows"]])
        ws_overview.append(["column names", ", ".join(meta["columns"])])
        if "xlsx_data_types" in meta:
            ws_overview.append(["xlsx data types", ", ".join(meta["xlsx_data_types"])])

        # Iterate through all cells in the first column (Column A)
        max_length = 0
        for cell in ws_overview["A"]:
            if cell.value:
                if len(str(cell.value)) > max_length:
                    max_length = len(str(cell.value))
        ws_overview.column_dimensions["A"].width = max_length + 2

        ws_data = wb.create_sheet(title="Data")
        ws_data.append(columns)
        for row in data:
            row = [str(row.get(col, "")) for col in columns]
            if xlsx_data_types:
                row = self._convert_row_to_xlsx_data_types(row, xlsx_data_types)
            ws_data.append(row)
        xlsx_buffer = io.BytesIO()
        wb.save(xlsx_buffer)
        xlsx_buffer.seek(0)
        return xlsx_buffer

    def convert_rows_to_csv(self, rows, fieldnames=None):
        f = io.StringIO(newline="")
        if fieldnames is None:
            fieldnames = sorted(list(rows[0].keys()))
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        f.seek(0)
        # read and make sure we don't have a BOM
        return f.read().lstrip("\ufeff")

    def convert_rows_to_tsv(self, rows, fieldnames=None):
        f = io.StringIO(newline="")
        if fieldnames is None:
            fieldnames = sorted(list(rows[0].keys()))
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)
        f.seek(0)
        # read and make sure we don't have a BOM
        return f.read().lstrip("\ufeff")

    def infer_latitude(self, rows):
        keys = POSSIBLE_LATITUDE_KEYS
        for row in rows:
            keys = [key for key in keys if key in row]
        return keys[0] if len(keys) > 1 else None

    def infer_longitude(self, rows):
        keys = POSSIBLE_LONGITUDE_KEYS
        for row in rows:
            keys = [key for key in keys if key in row]
        return keys[0] if len(keys) > 1 else None

    def convert_rows_to_geojson_points(self, rows, longitude_key, latitude_key):
        features = []
        for row in rows:
            features.append(
                {
                    "type": "Feature",
                    "properties": row,
                    "geometry": {
                        "type": "Point",
                        "coordinates": [row[longitude_key], row[latitude_key]],
                    },
                }
            )
        return {"type": "FeatureCollection", "features": features}

    def convert_rows_to_geojson_polygons(self, rows, polygon_key):
        features = []
        for row in rows:
            geometry = row[polygon_key]
            if isinstance(geometry, str):
                geometry = json_loads(geometry)
            if geometry["type"] in ["Polygon", "MultiPolygon"]:
                features.append(
                    {
                        "type": "Feature",
                        "properties": row,
                        "geometry": geometry,
                    }
                )
        return {"type": "FeatureCollection", "features": features}

    def update_dataset(
        self,
        name: str,
        version: str,
        data: list[dict],
        column_names: list[str] | None = None,
        description: str | None = None,
        polygon_key: str | None = None,
        latitude_key: str | None = None,
        longitude_key: str | None = None,
        json: bool = True,
        jsonl: bool = True,
        geojson: bool = True,
        parquet: bool = True,
        xlsx: bool = False,
        xlsx_data_types: list[str] | None = None,
    ):
        lastUpdated = dict(
            [(tz, datetime.now(ZoneInfo(tz)).isoformat()) for tz in self.timezones]
        )
        columns = column_names if column_names else self._get_unique_keys(data)
        meta = {
            "name": name,
            "lastUpdated": lastUpdated,
            "description": description or "",
            "numColumns": len(columns),
            "numRows": len(data),
            "columns": columns,
            "files": [],
        }

        flat_rows = self._flatten_rows(data)

        data_as_csv = self.convert_rows_to_csv(flat_rows, fieldnames=columns)
        data_as_tsv = self.convert_rows_to_tsv(flat_rows, fieldnames=columns)

        df = pd.DataFrame(data)

        data_as_geojson_points = None
        data_as_geojson_polygons = None
        shapefile_points_blob = None
        shapefile_polygons_blob = None

        if latitude_key and longitude_key:
            if geojson:
                data_as_geojson_points = self.convert_rows_to_geojson_points(
                    data, longitude_key=longitude_key, latitude_key=latitude_key
                )

            if parquet:
                gdf = gpd.GeoDataFrame(
                    df,
                    geometry=gpd.points_from_xy(df[longitude_key], df[latitude_key]),
                    crs="EPSG:4326",
                )
                # drops polygonal column that we don't use in shapefile output
                if polygon_key:
                    gdf.drop(columns=[polygon_key], inplace=True)
                buffer = io.BytesIO()
                gdf.to_parquet(buffer, engine="pyarrow")

                data_as_parquet_blob = buffer.getvalue()
            else:
                data_as_parquet_blob = None

            shapefile_points_blob = self.convert_gdf_to_shapefile(gdf)

        if polygon_key:
            if geojson:
                data_as_geojson_polygons = self.convert_rows_to_geojson_polygons(
                    data, polygon_key=polygon_key
                )

            if parquet:
                gdf = gpd.GeoDataFrame(
                    df,
                    geometry=df[polygon_key].apply(shape),
                    crs="EPSG:4326",
                )

                # can drop old polygon column, since we replaced with a geometry column
                gdf.drop(columns=[polygon_key], inplace=True)

                buffer = io.BytesIO()
                gdf.to_parquet(buffer, engine="pyarrow")
                data_as_parquet_blob = buffer.getvalue()
            else:
                data_as_parquet_blob = None

            shapefile_polygons_blob = self.convert_gdf_to_shapefile(gdf)

        if not ((latitude_key and longitude_key) or polygon_key):
            if parquet:
                buffer = io.BytesIO()
                df.to_parquet(buffer, engine="pyarrow")
                data_as_parquet_blob = buffer.getvalue()
            else:
                data_as_parquet_blob = None

        if xlsx:
            if xlsx_data_types:
                meta["xlsx_data_types"] = xlsx_data_types
            data_as_xlsx = self.convert_to_xlsx(
                meta, flat_rows, columns, xlsx_data_types
            )
            self.upload_xlsx(name, version, data_as_xlsx)
            meta["files"].append({"filename": "data.xlsx", "format": "Excel"})

        self.upload_csv(name, version, data_as_csv)
        meta["files"].append({"filename": "data.csv", "format": "CSV"})

        self.upload_tsv(name, version, data_as_tsv)
        meta["files"].append({"filename": "data.tsv", "format": "TSV"})

        if json:
            self.upload_json(name, version, data)
            meta["files"].append({"filename": "data.json", "format": "JSON"})

        if jsonl:
            self.upload_jsonl(name, version, data)
            meta["files"].append({"filename": "data.jsonl", "format": "JSON Lines"})

        if geojson and data_as_geojson_points:
            self.upload_geojson_points(name, version, data_as_geojson_points)
            meta["files"].append(
                {"filename": "data.points.geojson", "format": "GeoJSON (Points)"}
            )

        if geojson and data_as_geojson_polygons:
            self.upload_geojson_polygons(name, version, data_as_geojson_polygons)
            meta["files"].append(
                {"filename": "data.polygons.geojson", "format": "GeoJSON (Polygons)"}
            )

        if shapefile_points_blob:
            self.upload_shapefile_points(name, version, shapefile_points_blob)
            meta["files"].append(
                {"filename": "data.points.shp.zip", "format": "Shapefile (Points)"}
            )

        if shapefile_polygons_blob:
            self.upload_shapefile_polygons(name, version, shapefile_polygons_blob)
            meta["files"].append(
                {"filename": "data.polygons.shp.zip", "format": "Shapefile (Polygons)"}
            )

        if parquet and data_as_parquet_blob:
            self.upload_parquet(name, version, data_as_parquet_blob)
            meta["files"].append({"filename": "data.parquet", "format": "Parquet"})

        self.upload_metadata(name, version, meta)
