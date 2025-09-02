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
