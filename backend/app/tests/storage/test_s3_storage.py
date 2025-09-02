import io
from datetime import datetime, timedelta
import pandas as pd
import logging
import pytest

from app.services.storage.s3_storage import S3Storage

class FakeS3Client:
    def __init__(self):
        # store put_object calls as dict[(Bucket,Key)] = bytes
        self.store = {}
        self.list_response = {}
        self.get_map = {}

    def put_object(self, Bucket, Key, Body):
        # Body may be bytes or a file-like; normalize to bytes
        if hasattr(Body, "read"):
            content = Body.read()
        else:
            content = Body
        self.store[(Bucket, Key)] = content

    def list_objects_v2(self, Bucket, Prefix):
        return self.list_response

    def get_object(self, Bucket, Key):
        if (Bucket, Key) not in self.get_map:
            raise Exception("NoSuchKey")
        return {"Body": io.BytesIO(self.get_map[(Bucket, Key)])}
    
def make_csv_bytes(df):
    return df.to_csv(index=False).encode("utf-8")

def create_load_files(monkeypatch):
    fake = FakeS3Client()
    now = datetime.now()
    t_old = now - timedelta(seconds=100)
    t_new = now

    fake.list_response = {
        "Contents": [
            {"Key": "pfx/players_old.csv", "LastModified": t_old},
            {"Key": "pfx/players_new.csv", "LastModified": t_new},
        ]
    }

    csv_old = make_csv_bytes(pd.DataFrame({"val": [1]}))
    csv_new = make_csv_bytes(pd.DataFrame({"val": [2]}))
    fake.get_map[("b", "pfx/players_old.csv")] = csv_old
    fake.get_map[("b", "pfx/players_new.csv")] = csv_new

    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

def test_save_success(monkeypatch):
    fake = FakeS3Client()
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    storage = S3Storage(s3_bucket="my-bucket")
    ok = storage.save({"players": pd.DataFrame({"a": [1]})}, prefix="pfx")
    assert ok is True

    # confirm csv and json keys were created in store
    keys = [k for (b, k) in fake.store.keys() if b == "my-bucket"]
    assert any(k.endswith(".csv") and "players" in k for k in keys)
    assert any(k.endswith(".json") and "players" in k for k in keys)

def test_save_skips_empty_df(monkeypatch):
    fake = FakeS3Client()
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    storage = S3Storage(s3_bucket="my-bucket")
    ok = storage.save({"players": pd.DataFrame({"a": []})}, prefix="pfx")
    assert ok is True
    assert len(fake.store) == 0

def test_save_not_configured(monkeypatch):
    fake = FakeS3Client()
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    storage = S3Storage(s3_bucket=None)
    ok = storage.save({"players": pd.DataFrame({"a": [1]})}, prefix="pfx")
    assert ok is False
    assert len(fake.store) == 0

def test_fail_save_exception(monkeypatch):
    class FailingS3Client(FakeS3Client):
        def put_object(self, Bucket, Key, Body):
            raise Exception("Simulated failure")

    fake = FailingS3Client()
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    storage = S3Storage(s3_bucket="my-bucket")
    ok = storage.save({"players": pd.DataFrame({"a": [1]})}, prefix="pfx")
    assert ok is False
    assert len(fake.store) == 0

def test_load_not_configured(caplog):
    storage = S3Storage(s3_bucket=None)
    caplog.set_level(logging.ERROR)
    res = storage.load(prefix="pfx")
    assert res == {}
    assert "S3 client or bucket not configured" in caplog.text

def test_load_no_objects(monkeypatch, caplog):
    fake = FakeS3Client()
    fake.list_response = {"KeyCount": 0}
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    bucket = "my-bucket"
    prefix = "pfx"

    storage = S3Storage(s3_bucket=bucket)
    caplog.set_level(logging.INFO)
    res = storage.load(prefix=prefix)
    assert res == {}
    assert f"No objects found in S3 bucket {bucket} with prefix {prefix}." in caplog.text

def test_load_latest(monkeypatch):
    create_load_files(monkeypatch)

    storage = S3Storage(s3_bucket="b")
    res = storage.load(prefix="pfx", latest_only=True)
    assert "players" in res
    df = res["players"]

    assert len(df) == 1
    assert df["val"].iloc[0] == 2

def test_load_first(monkeypatch):
    create_load_files(monkeypatch)

    storage = S3Storage(s3_bucket="b")
    res = storage.load(prefix="pfx", latest_only=False)
    assert "players" in res
    df = res["players"]

    assert len(df) == 1
    assert df["val"].iloc[0] == 1

def test_fail_load_exception(monkeypatch, caplog):
    class FailingS3Client(FakeS3Client):
        def list_objects_v2(self, Bucket, Prefix):
            raise Exception("Simulated failure")

    fake = FailingS3Client()
    monkeypatch.setattr("app.services.storage.s3_storage.boto3.client", lambda *a, **k: fake)

    storage = S3Storage(s3_bucket="b")
    caplog.set_level(logging.ERROR)
    res = storage.load(prefix="pfx")
    assert res == {}
    assert "Failed to load data from S3" in caplog.text