"""Tests for Feast feature store configuration"""

import os

import yaml


class TestFeatureStoreConfig:
    def test_feature_store_yaml_exists(self):
        config_path = os.path.join(os.path.dirname(__file__), "..", "feast", "feature_store.yaml")
        assert os.path.exists(config_path)

    def test_feature_store_yaml_valid(self):
        config_path = os.path.join(os.path.dirname(__file__), "..", "feast", "feature_store.yaml")
        with open(config_path) as f:
            config = yaml.safe_load(f)
        assert "project" in config
        assert config["project"] == "nyc_taxi_features"

    def test_feature_store_has_registry(self):
        config_path = os.path.join(os.path.dirname(__file__), "..", "feast", "feature_store.yaml")
        with open(config_path) as f:
            config = yaml.safe_load(f)
        assert "registry" in config


class TestFeatureDefinitions:
    def test_entities_defined(self):
        from aide2.feast.features import taxi_zone, vendor, time_bucket

        assert taxi_zone.name == "taxi_zone"
        assert vendor.name == "vendor"
        assert time_bucket.name == "time_bucket"

    def test_feature_views_defined(self):
        from aide2.feast.features import (
            hourly_trip_features,
            zone_features,
            daily_trip_features,
        )

        assert hourly_trip_features.name == "hourly_trip_features"
        assert zone_features.name == "zone_features"
        assert daily_trip_features.name == "daily_trip_features"

    def test_hourly_features_fields(self):
        from aide2.feast.features import hourly_trip_features

        field_names = [f.name for f in hourly_trip_features.schema]
        assert "avg_fare" in field_names
        assert "trip_count" in field_names
        assert "total_revenue" in field_names

    def test_zone_features_ttl(self):
        from aide2.feast.features import zone_features

        assert zone_features.ttl.days == 7


class TestFeatureServing:
    def test_serve_app_importable(self):
        from aide2.feast.serve import app

        assert app is not None
        assert app.title == "NYC Taxi Feast API"

    def test_health_endpoint(self):
        from fastapi.testclient import TestClient
        from aide2.feast.serve import app

        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200


class TestIngestion:
    def test_nyc_taxi_url_format(self):
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        year, month = 2024, 1
        filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
        url = f"{base_url}/{filename}"
        assert "2024-01" in url
        assert url.endswith(".parquet")

    def test_minio_bucket_name(self):
        bucket = "raw-data"
        key_prefix = "nyc-taxi/"
        assert "/" not in bucket
        assert key_prefix.endswith("/")
