from typing import List

class NBASettings:
    """
    Centralized settings for NBA Analytics App
    """

    DEFAULT_SEASON: str = "2023-24"

    DEFAULT_SEASONS_LIST: List[str] = [
        "2021-22",
        "2022-23",
        "2023-24"
    ]

    DEFAULT_DATA_FORMAT: str = "csv"
