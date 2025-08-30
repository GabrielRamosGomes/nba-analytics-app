import sys
from pathlib import Path

# Ensure the parent directory is in the sys.path for imports
sys.path.append(str(Path(__file__).resolve().parent.parent))

backend_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(backend_dir))

from app.services.nba.nba_api_client import NBAApiClient
from app.services.nba.nba_settings import NBASettings
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_nba_dataset():
    """ Setup NBA dataset by collecting and storing data """
    logger.info("Starting NBA dataset setup...")

    s3_bucket = NBASettings.get_s3_data_bucket()
    client = NBAApiClient(s3_bucket)
    seasons = NBASettings.DEFAULT_SEASONS_LIST
    
    try:
        logger.info("Collecting and storing NBA dataset...")
        source = "local"  # Change to "s3" if you want to load from S3
        success = client.collect_and_store_dataset(
            seasons=seasons,
            source=source,
        )

        if not success:
            logger.error("Failed to collect and store NBA dataset.")
            return False
        
        dataset = client.load_dataset(source=source)

        # Print summary
        logger.info("\nDataset Collection Summary:")
        logger.info("=" * 50)
        
        for name, df in dataset.items():
            if not df.empty:
                logger.info(f"{name.upper()}:")
                logger.info(f"  - Records: {len(df):,}")
                logger.info(f"  - Columns: {len(df.columns)}")
                
                if 'SEASON' in df.columns:
                    seasons_in_data = df['SEASON'].unique()
                    logger.info(f"  - Seasons: {list(seasons_in_data)}")
                
                if 'PLAYER_NAME' in df.columns:
                    player_count = df['PLAYER_NAME'].nunique()
                    logger.info(f"  - Unique Players: {player_count}")
                
                logger.info("")
        
        logger.info("NBA dataset setup completed successfully!")
        return True

    except Exception as e:
        logger.error(f"Failed to setup NBA dataset: {e}")
        return False

if __name__ == "__main__":
    success = setup_nba_dataset()
    """ Setup NBA dataset by collecting and storing data """
    if success:
        print("\n✅ NBA dataset is ready for your analytics app!")
    else:
        print("\n❌ Failed to setup NBA dataset. Check logs for details.")
