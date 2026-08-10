"""Tests for Feast feature store configuration"""

import os

import pytest
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


class TestComputeFareRatio:
    """
    Real tests for the on-demand feature transform logic. Calling the
    ``@on_demand_feature_view``-decorated ``fare_prediction_features`` was never
    exercised by any prior test; it triggered a real ``RecursionError`` from a
    pandas 1.5.3/numpy 1.26.3 incompatibility in ``Series.replace(0, pd.NA)``.
    That's fixed (see features.py) and covered here via the extracted pure helper.
    """

    def test_computes_ratio_of_fare_to_zone_average(self):
        import pandas as pd

        from aide2.feast.features import _compute_fare_ratio

        zone_df = pd.DataFrame({"zone_avg_fare": [10.0]})
        req_df = pd.DataFrame({"fare_amount": [25.0], "location_id": [1]})
        out = _compute_fare_ratio(zone_df, req_df)
        assert out["expected_fare_ratio"].iloc[0] == pytest.approx(2.5)
        assert out["location_id"].iloc[0] == 1

    def test_zero_zone_average_guards_against_division_by_zero(self):
        import pandas as pd

        from aide2.feast.features import _compute_fare_ratio

        zone_df = pd.DataFrame({"zone_avg_fare": [0.0]})
        req_df = pd.DataFrame({"fare_amount": [10.0], "location_id": [2]})
        out = _compute_fare_ratio(zone_df, req_df)
        assert pd.isna(out["expected_fare_ratio"].iloc[0])

    def test_decorated_object_is_a_real_on_demand_feature_view(self):
        """
        The ``@on_demand_feature_view`` decorator returns a real Feast
        ``OnDemandFeatureView`` object (not the original function) -- Feast calls
        the underlying transform internally via the feature-retrieval APIs, not
        by invoking the decorated name directly. ``_compute_fare_ratio`` (tested
        above) is what's directly callable.
        """
        from feast.on_demand_feature_view import OnDemandFeatureView

        from aide2.feast.features import fare_prediction_features

        assert isinstance(fare_prediction_features, OnDemandFeatureView)
        assert fare_prediction_features.name == "fare_prediction_features"


class FakeFeatureStore:
    """Minimal stand-in exposing only the methods serve.py's endpoints call."""

    def __init__(
        self,
        online_result=None,
        historical_df=None,
        feature_views=None,
        on_demand_views=None,
        raise_on=None,
    ):
        self._online_result = online_result
        self._historical_df = historical_df
        self._feature_views = feature_views or []
        self._on_demand_views = on_demand_views or []
        self._raise_on = raise_on or set()

    def get_online_features(self, features, entity_rows, full_feature_names=True):
        if "online" in self._raise_on:
            raise RuntimeError("boom")
        return self._online_result

    def get_historical_features(self, entity_df, features, full_feature_names=True):
        if "historical" in self._raise_on:
            raise RuntimeError("boom")

        class _Job:
            def __init__(self, df):
                self._df = df

            def to_df(self):
                return self._df

        return _Job(self._historical_df)

    def list_feature_views(self):
        if "list" in self._raise_on:
            raise RuntimeError("boom")
        return self._feature_views

    def list_on_demand_feature_views(self):
        return self._on_demand_views


class FakeOnlineResponse:
    def __init__(self, data):
        self._data = data

    def to_dict(self, include_event_timestamps=True):
        return self._data


class TestGetStore:
    def setup_method(self):
        from aide2.feast import serve

        serve._store = None

    def teardown_method(self):
        from aide2.feast import serve

        serve._store = None

    def test_returns_store_on_success(self, monkeypatch):
        import feast

        from aide2.feast import serve

        sentinel = object()
        monkeypatch.setattr(feast, "FeatureStore", lambda repo_path: sentinel)
        assert serve.get_store() is sentinel

    def test_caches_store_across_calls(self, monkeypatch):
        import feast

        from aide2.feast import serve

        calls = []

        def fake_store(repo_path):
            calls.append(repo_path)
            return object()

        monkeypatch.setattr(feast, "FeatureStore", fake_store)
        serve.get_store()
        serve.get_store()
        assert len(calls) == 1

    def test_raises_503_when_store_init_fails(self, monkeypatch):
        import feast
        from fastapi import HTTPException

        from aide2.feast import serve

        def boom(repo_path):
            raise RuntimeError("registry not found")

        monkeypatch.setattr(feast, "FeatureStore", boom)
        with pytest.raises(HTTPException) as exc_info:
            serve.get_store()
        assert exc_info.value.status_code == 503


class TestFieldsToJson:
    def test_converts_fields_with_dtype(self):
        from aide2.feast.serve import _fields_to_json

        class _Field:
            def __init__(self, name, dtype):
                self.name = name
                self.dtype = dtype

        out = _fields_to_json([_Field("avg_fare", "Float64"), _Field("trip_count", "Int64")])
        assert out == [
            {"name": "avg_fare", "dtype": "Float64"},
            {"name": "trip_count", "dtype": "Int64"},
        ]


class TestServeEndpoints:
    def setup_method(self):
        from fastapi.testclient import TestClient

        from aide2.feast import serve

        self.serve = serve
        self.client = TestClient(serve.app)

    def test_metrics_endpoint_returns_prometheus_text(self):
        response = self.client.get("/metrics")
        assert response.status_code == 200
        assert "feast_api_requests_total" in response.text

    def test_features_online_success(self, monkeypatch):
        fake_store = FakeFeatureStore(online_result=FakeOnlineResponse({"avg_fare": [12.0]}))
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.post(
            "/features/online",
            json={"feature_refs": ["zone_features:zone_avg_fare"], "entity_rows": [{"location_id": 1}]},
        )
        assert response.status_code == 200
        assert response.json()["features"]["avg_fare"] == [12.0]

    def test_features_online_error_returns_400(self, monkeypatch):
        fake_store = FakeFeatureStore(raise_on={"online"})
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.post(
            "/features/online",
            json={"feature_refs": ["x"], "entity_rows": [{"location_id": 1}]},
        )
        assert response.status_code == 400

    def test_features_historical_empty_entity_rows_returns_400(self, monkeypatch):
        fake_store = FakeFeatureStore()
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.post(
            "/features/historical", json={"feature_refs": ["x"], "entity_rows": []}
        )
        assert response.status_code == 400

    def test_features_historical_success_defaults_event_timestamp(self, monkeypatch):
        import pandas as pd

        fake_store = FakeFeatureStore(historical_df=pd.DataFrame({"avg_fare": [9.5]}))
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.post(
            "/features/historical",
            json={"feature_refs": ["x"], "entity_rows": [{"location_id": 1}]},
        )
        assert response.status_code == 200
        assert response.json()["dataframe"]["avg_fare"] == [9.5]

    def test_features_historical_error_returns_400(self, monkeypatch):
        fake_store = FakeFeatureStore(raise_on={"historical"})
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.post(
            "/features/historical",
            json={"feature_refs": ["x"], "entity_rows": [{"location_id": 1}]},
        )
        assert response.status_code == 400

    def test_features_list_filters_by_type_and_serializes_schema(self, monkeypatch):
        from feast.feature_view import FeatureView
        from feast.on_demand_feature_view import OnDemandFeatureView

        from aide2.feast.features import zone_features, fare_prediction_features

        assert isinstance(zone_features, FeatureView)
        fake_store = FakeFeatureStore(
            feature_views=[zone_features],
            on_demand_views=[
                v for v in [fare_prediction_features] if isinstance(v, OnDemandFeatureView)
            ]
            or [],
        )
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.get("/features/list")
        assert response.status_code == 200
        body = response.json()
        names = [v["name"] for v in body["feature_views"]]
        assert "zone_features" in names
        odfv_names = [v["name"] for v in body["on_demand_feature_views"]]
        assert "fare_prediction_features" in odfv_names

    def test_features_list_error_returns_500(self, monkeypatch):
        fake_store = FakeFeatureStore(raise_on={"list"})
        monkeypatch.setattr(self.serve, "get_store", lambda: fake_store)
        response = self.client.get("/features/list")
        assert response.status_code == 500
