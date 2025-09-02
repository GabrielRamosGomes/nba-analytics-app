import pandas as pd
import boto3
from datetime import datetime
from typing import Dict, Optional
import logging

from .base_storage import BaseStorage

logger = logging.getLogger(__name__)

class S3Storage(BaseStorage):
    """
    AWS S3 storage implementation.
    """

    def __init__(self, s3_bucket: str):
        self.s3_bucket = s3_bucket
        self.s3_client = boto3.client('s3') if s3_bucket else None


    def save(self, dataset: Dict[str, pd.DataFrame], prefix: str = "nba-data") -> bool:
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
        
    def load(self, prefix: str = "nba-data", latest_only: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Load the dataset from S3.
        """
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
                    latest_file = max(files, key=lambda x: x['modified'])
                    key = latest_file['Key']
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