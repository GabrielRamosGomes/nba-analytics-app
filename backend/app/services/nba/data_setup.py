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

    client = NBAApiClient()
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
