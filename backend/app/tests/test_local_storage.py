import logging
import os
import io
import time
import pandas as pd
import pytest

from ..services.storage.local_storage import LocalStorage

def make_df():
    return pd.DataFrame({
        "A": [1, 2],
        "B": ["x", "y"]
    })

def create_load_files(base_path):
    pfx_dir = os.path.join(base_path, "pfx")
    os.makedirs(pfx_dir, exist_ok=True)

    # create two csv files for same dataset name "players"
    path1 = os.path.join(pfx_dir, "players_old.csv")
    path2 = os.path.join(pfx_dir, "players_new.csv")
    pd.DataFrame({"a": [1]}).to_csv(path1, index=False)
    pd.DataFrame({"a": [2]}).to_csv(path2, index=False)

    # set mod times so path2 is newer
    now = time.time()
    os.utime(path1, (now - 100, now - 100))
    os.utime(path2, (now, now))

def test_save_and_load(tmp_path):
    base = str(tmp_path)
    storage = LocalStorage(base_directory=base)
    prefix = "test-data"

    dataset = { "players": make_df() }
    ok = storage.save(dataset=dataset, prefix=prefix)
    assert ok is True

    dirpath = os.path.join(base, prefix)
    files = os.listdir(dirpath)

    assert any(f.startswith("players_") and f.endswith(".csv") for f in files)
    assert any(f.startswith("players_") and f.endswith(".json") for f in files)

    loaded = storage.load(prefix=prefix, latest_only=True)
    assert "players" in loaded
    pd.testing.assert_frame_equal(loaded["players"].reset_index(drop=True), dataset["players"].reset_index(drop=True))

def test_load_no_directory(tmp_path):
    base = str(tmp_path)
    storage = LocalStorage(base_directory=base)
    prefix = "nonexistent-data"

    loaded = storage.load(prefix=prefix, latest_only=True)
    assert loaded == {}

def test_save_skips_empty_df(tmp_path):
    base = str(tmp_path)
    storage = LocalStorage(base_directory=base)
    prefix = "empty-data"

    dataset = { "players": pd.DataFrame() }
    ok = storage.save(dataset=dataset, prefix=prefix)
    assert ok is True

    dirpath = os.path.join(base, prefix)
    
    if os.path.exists(dirpath):
        files = os.listdir(dirpath)
        assert len(files) == 0

def test_failed_save(tmp_path, monkeypatch):
    base = str(tmp_path)
    storage = LocalStorage(base_directory=base)
    prefix = "fail-data"

    def fail_makedirs(*args, **kwargs):
        raise OSError("Simulated failure")

    monkeypatch.setattr(os, "makedirs", fail_makedirs)

    dataset = { "players": make_df() }
    ok = storage.save(dataset=dataset, prefix=prefix)
    assert ok is False

def test_load_latest(tmp_path):
    base = str(tmp_path)
    create_load_files(base_path=base)

    storage = LocalStorage(base_directory=base)

    # latest_only True -> should return the newer file (value 2)
    res_latest = storage.load(prefix="pfx", latest_only=True)
    assert "players" in res_latest
    assert int(res_latest["players"]["a"].iloc[0]) == 2
    
def test_load_all(tmp_path, monkeypatch):
    base = str(tmp_path)
    create_load_files(base_path=base)

    storage = LocalStorage(base_directory=base)
    
    # latest_only False -> branch uses files[0]; control os.listdir order deterministically
    monkeypatch.setattr("os.listdir", lambda d: ["players_old.csv", "players_new.csv"])
    res_all = storage.load(prefix="pfx", latest_only=False)
    assert "players" in res_all
    assert int(res_all["players"]["a"].iloc[0]) == 1

def test_load_catch_exception(tmp_path, monkeypatch, caplog):
    base = str(tmp_path)
    prefix = "pfx"
    dirpath = os.path.join(base, prefix)
    os.makedirs(dirpath, exist_ok=True)
    csv_path = os.path.join(dirpath, "players_20250101_000000.csv")
    pd.DataFrame({"a": [1]}).to_csv(csv_path, index=False)

    storage = LocalStorage(base_directory=base)

    # Force pd.read_csv used inside local_storage to raise an exception
    monkeypatch.setattr(
        "app.services.storage.local_storage.pd.read_csv",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    caplog.set_level(logging.ERROR)
    result = storage.load(prefix=prefix, latest_only=True)

    assert result == {}
    assert "Failed to load data from local" in caplog.text