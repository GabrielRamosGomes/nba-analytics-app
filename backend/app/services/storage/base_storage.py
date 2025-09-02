from abc import ABC, abstractmethod
from typing import Dict
import pandas as pd


class BaseStorage(ABC):
    """
    Abstract base class for storage implementations.
    """
    @abstractmethod
    def save(self, dataset: Dict[str, pd.DataFrame], prefix: str) -> bool:
        pass
    
    @abstractmethod
    def load(self, prefix: str, latest_only: bool) -> Dict[str, pd.DataFrame]:
        pass
