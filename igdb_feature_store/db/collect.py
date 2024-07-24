import os
from typing import List
from igdb_feature_store.db.ingestor import IGDBIngestor, get_twitch_token


def collect_igdb_data(endpoints: List[str], delay: int):
    client_id = os.environ["IGDB_API_ID"]
    client_secret = os.environ["IGDB_API_SECRET"]
    twitch_token = get_twitch_token(client_id, client_secret)

    db_home = os.environ["IGDB_DB_HOME"]
    path = f"{db_home}/raw"

    ingestor = IGDBIngestor(
        token=twitch_token, client_id=client_id, delay=delay, path=path
    )

    for endpoint in endpoints:
        ingestor.process(endpoint=endpoint)


if __name__ == "__main__":
    endpoints = [
        "characters",
        "collections",
        "companies",
        "franchises",
        "games",
        "game_modes",
        "game_versions",
        "genres",
        "languages",
        "language_supports",
        "platforms",
        "platform_families",
        "platform_versions",
        "regions",
        "themes",
    ]
    collect_igdb_data(endpoints=endpoints, delay=3450)
