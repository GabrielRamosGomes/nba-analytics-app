import pandas as pd
import os
from datetime import datetime
from typing import Dict
import logging

from .base_storage import BaseStorage

logger = logging.getLogger(__name__)

class LocalStorage(BaseStorage):
    """
    Handles local data storage operations
    """

    def __init__(self, base_directory: str = "data"):
        self.base_directory = base_directory

    def save(self, dataset: Dict[str, pd.DataFrame], prefix: str = "nba-data") -> bool:
        """
        Save the dataset to local file system.
        """

        try:
            directory = os.path.join(self.base_directory, prefix)
            os.makedirs(directory, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

            for name, df in dataset.items():
                if df.empty:
                    continue

                csv_path = os.path.join(directory, f"{name}_{timestamp}.csv")
                json_path = os.path.join(directory, f"{name}_{timestamp}.json")
                
                df.to_csv(csv_path, index=False)
                df.to_json(json_path, orient="records", indent=2)

                logger.info(f"Saved {name} locally as {csv_path} and {json_path}")

            return True
        except Exception as e:
            logger.error(f"Failed to save data locally: {e}")
            return False

    def load(self, prefix: str = "nba-data", latest_only: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Load the dataset from local file system.
        """
        dataset = {}
        
        try:
            directory = os.path.join(self.base_directory, prefix)
            
            if not os.path.exists(directory):
                logger.warning(f"Directory {directory} does not exist.")
                return dataset

            csv_files = {}
            
            # Group files by dataset name
            for filename in os.listdir(directory):
                if filename.endswith(".csv"):
                    file_path = os.path.join(directory, filename)
                    dataset_name = filename.replace('.csv', '').split('_')[0]
                    
                    if dataset_name not in csv_files:
                        csv_files[dataset_name] = []
                    
                    csv_files[dataset_name].append({
                        'path': file_path,
                        'modified': os.path.getmtime(file_path)
                    })

            # Load datasets (latest if specified)
            for dataset_name, files in csv_files.items():
                if latest_only:
                    latest_file = max(files, key=lambda x: x['modified'])
                    file_path = latest_file['path']
                else:
                    # Just take the first file
                    file_path = files[0]['path']
                
                df = pd.read_csv(file_path)
                dataset[dataset_name] = df
                
                logger.info(f"Loaded {dataset_name} from {file_path}")
                
            return dataset
        except Exception as e:
            logger.error(f"Failed to load data from local: {e}")
            return dataset