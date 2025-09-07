import threading
import time
from typing import Any, Dict, Optional, Tuple

class Cache:
    """ Simple thread-safe in-memory global cache with optional TTL support."""

    def __init__(self):
        self._lock = threading.Lock()
        self._store: Dict[str, Tuple[Any, Optional[float]]] = {}

    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """ Set a value in the cache with an optional TTL (in seconds). """
        expiry = None if ttl is None else (time.time() + float(ttl))
        with self._lock:
            self._store[key] = (value, expiry)
    
    def get(self, key: str, default: Any = None) -> Optional[Any]:
        """ Get a value from the cache. Returns None if not found or expired. """
        with self._lock:
            item = self._store.get(key)
            if item is None:
                return default
            value, expiry = item
            if expiry is not None and time.time() > expiry:
                del self._store[key]
                return default
            return value
    
    def has(self, key: str) -> bool:
        """ Check if a key exists and is not expired. """
        return self.get(key, default=None) is not None
    
    def delete(self, key: str):
        """ Delete a key from the cache. """
        with self._lock:
            if key in self._store:
                del self._store[key]

cache = Cache()