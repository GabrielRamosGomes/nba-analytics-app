import pandas as pd
import pandas.testing as pdt

from app.services.nba.nba_api_client import NBAApiClient

COLLECTOR_PATH = "app.services.nba.nba_api_client.NBADataCollector"

def make_fake_collector(dataset=None):
    """Return a FakeCollector class (not instance) so NBAApiClient can call it."""
    default = {"players": pd.DataFrame({"name": ["Player1", "Player2"]})}

    class FakeCollector:
        def collect_dataset(self, seasons=None):
            return dataset if dataset is not None else default

    return FakeCollector

def make_fake_storage(initial_load=None, save_result=True):
    """Return a FakeStorage class that records save calls and can provide load."""
    class FakeStorage:
        def __init__(self):
            self.saved = None
            # allow load() to be controllable for other tests
            self._load_value = initial_load if initial_load is not None else {}
            self._save_result = save_result

        def save(self, dataset, prefix):
            self.saved = {"dataset": dataset, "prefix": prefix}
            return self._save_result

        def load(self, prefix, latest_only):
            return self._load_value

    return FakeStorage()

def test_collect_and_store_dataset_success(monkeypatch):
    collector = make_fake_collector()
    monkeypatch.setattr(COLLECTOR_PATH, collector)

    fake_storage = make_fake_storage()
    client = NBAApiClient(storage=fake_storage)
    success = client.collect_and_store_dataset(seasons=["2023-24"], prefix="test-prefix")

    assert success is True
    assert fake_storage.saved is not None
    assert fake_storage.saved["prefix"] == "test-prefix"

    saved_df = fake_storage.saved["dataset"]["players"]
    expected_df = pd.DataFrame({"name": ["Player1", "Player2"]})
    pdt.assert_frame_equal(saved_df.reset_index(drop=True), expected_df.reset_index(drop=True))

def test_collect_and_store_dataset_no_data(monkeypatch):
    collector = make_fake_collector(dataset={})
    monkeypatch.setattr(COLLECTOR_PATH, collector)

    fake_storage = make_fake_storage()
    client = NBAApiClient(storage=fake_storage)
    success = client.collect_and_store_dataset(seasons=["2023-24"], prefix="test-prefix")

    assert success is False

def test_setup_dataset_success(monkeypatch):
    collector = make_fake_collector()
    monkeypatch.setattr(COLLECTOR_PATH, collector)

    fake_storage = make_fake_storage()
    client = NBAApiClient(storage=fake_storage)
    success = client.setup_nba_dataset(seasons=["2022-23"], prefix="nba-test")

    assert success is True

def test_setup_dataset_failure(monkeypatch):
    collector = make_fake_collector()
    monkeypatch.setattr(COLLECTOR_PATH, collector)

    fake_storage = make_fake_storage(save_result=False)
    client = NBAApiClient(storage=fake_storage)
    success = client.setup_nba_dataset(seasons=["2022-23"], prefix="nba-test")

    assert success is False

def test_setup_dataset_exception(monkeypatch):
    class BadCollector:
        def collect_dataset(self, seasons=None):
            raise RuntimeError("Collection error")

    monkeypatch.setattr(COLLECTOR_PATH, BadCollector)

    fake_storage = make_fake_storage(save_result=False)
    client = NBAApiClient(storage=fake_storage)
    success = client.setup_nba_dataset(seasons=["2022-23"], prefix="nba-test")

    assert success is False

def test_load_data_success():
    expected_data = {
        "players": pd.DataFrame({"name": ["PlayerA", "PlayerB"]}),
        "teams": pd.DataFrame({"team": ["TeamX", "TeamY"]})
    }
    fake_storage = make_fake_storage(initial_load=expected_data)
    client = NBAApiClient(storage=fake_storage)

    loaded_data = client.load_data(prefix="nba-data", latest_only=True)

    assert loaded_data == expected_data
    assert client.cached_data == expected_data

def test_load_data_no_data():
    fake_storage = make_fake_storage(initial_load={})
    client = NBAApiClient(storage=fake_storage)

    loaded_data = client.load_data(prefix="nba-data", latest_only=True)

    assert loaded_data == {}
    assert client.cached_data == {}

def test_load_data_exception():
    class BadStorage:
        def load(self, prefix, latest_only):
            raise RuntimeError("Load error")

    fake_storage = BadStorage()
    client = NBAApiClient(storage=fake_storage)

    loaded_data = client.load_data(prefix="nba-data", latest_only=True)

    assert loaded_data == {}
    assert client.cached_data == {}