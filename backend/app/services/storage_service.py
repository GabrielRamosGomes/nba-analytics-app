import pandas as pd
import boto3
import json
import os 
from datetime import datetime
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class StorageService:
    """
    Handles data storage operations (local and S3)
    """

    def __init__(self, s3_bucket: Optional[str] = None):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3') if s3_bucket else None

    def save_to_s3(self, dataset: Dict[str, pd.DataFrame], prefix: str = "nba-data") -> bool:

        if not self.s3_client or not self.s3_bucket:
            logger.error("S3 client or bucket not configured.")
            return False
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            for name, df in dataset.items():
                if df.empty:
                    continue

                # Save as CSV
                csv_key = f"{prefix}/{name}_{timestamp}.csv"
                csv_buffer = df.to_csv(index=False).encode('utf-8')
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=csv_key,
                    Body=csv_buffer,
                )

                # Save as JSON for easier querying

                json_key = f"{prefix}/{name}_{timestamp}.json"
                json_buffer = df.to_json(orient="records", indent=2).encode('utf-8')
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=json_key,
                    Body=json_buffer,
                )

                logger.info(f"Saved {name} to S3 as {csv_key} and {json_key}")

            return True
        except Exception as e:
            logger.error(f"Failed to save data to S3: {e}")
            return False
    
    def save_to_local(self, dataset: Dict[str, pd.DataFrame], directory: str = "data") -> bool:
        try:
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
        
    def load_from_local(self, directory: str = "data", file_pattern: str = None) -> Dict[str, pd.DataFrame]:
        dataset = {}
        
        try:
            if not os.path.exists(directory):
                logger.warning(f"Directory {directory} does not exist.")
                return dataset

            # Find CSV files
            for filename in os.listdir(directory):
                if filename.endswith(".csv") and (file_pattern is None or file_pattern in filename):
                    file_path = os.path.join(directory, filename)
                    
                    name = filename.replace('.csv', '').split('_')[0]
                    
                    df = pd.read_csv(file_path)
                    dataset[name] = df
                    
                    logger.info(f"Loaded {name} from {file_path}")
                
            return dataset
        except Exception as e:
            logger.error(f"Failed to load data from local: {e}")
            return dataset
        
    def load_from_s3(self, prefix: str = "nba-data", latest_only: bool = True) -> Dict[str, pd.DataFrame]:
        dataset = {}

        if not self.s3_client or not self.s3_bucket:
            logger.error("S3 client or bucket not configured.")
            return dataset
        
        try:
            # List objects in the specified S3 bucket and prefix
            response = self.s3_client.list_objects_v2(Bucket=self.s3_bucket, Prefix=prefix)

            if 'Contents' not in response:
                logger.warning(f"No objects found in S3 bucket {self.s3_bucket} with prefix {prefix}.")
                return dataset
            
            csv_files = {}
            # Group files by name
            for obj in response['Contents']:
                key = obj['Key']
                if key.endswith('.csv'):
                    filename = key.split('/')[-1]
                    dataset_name = filename.split('_')[0]
                    
                    if dataset_name not in csv_files:
                        csv_files[dataset_name] = []

                    csv_files[dataset_name].append({
                        'Key': key,
                        'modified': obj['LastModified']
                    })

            # Load datasets (latest if specified)
            for dataset_name, files in csv_files.items():
                if latest_only:
                    lastest_file = max(files, key=lambda x: x['modified'])
                    key = lastest_file['Key']
                else:
                    # Just take the first file
                    key = files[0]['Key']
                
                # Download and load the CSV
                response = self.s3_client.get_object(Bucket=self.s3_bucket, Key=key)
                df = pd.read_csv(response['Body'])
                dataset[dataset_name] = df
                logger.info(f"Loaded {dataset_name} from S3 key {key}")
            
            return dataset
        except Exception as e:
            logger.error(f"Failed to load data from S3: {e}")
            return dataset
    
