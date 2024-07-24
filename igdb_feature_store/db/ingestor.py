import os
import json
from typing import Dict
import requests
from datetime import datetime, timedelta


def get_twitch_token(client_id: str, client_secret: str) -> str:

    params = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }
    url = "https://id.twitch.tv/oauth2/token"
    response = requests.post(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data["access_token"]


class Ingestor:

    def __init__(self, token: str, client_id: str, delay: int, path: str):

        self.token = token
        self.client_id = client_id
        self.delay = delay
        self.path = path

        self.base_url = "https://api.igdb.com/v4"
        self.headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
        self.delay_timestamp = int((datetime.now() - timedelta(days=delay)).timestamp())

    def get_data(self, endpoint: str, params: Dict = {}) -> Dict:
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        return response.json()

    def save_data(self, data: Dict, endpoint: str) -> bool:

        folder = os.path.join(self.path, endpoint)
        os.makedirs(folder, exist_ok=True)

        name = datetime.now().strftime("%Y%m%d_%H%M%S.%f")
        file_path = os.path.join(
            folder,
            f"{name}.json",
        )

        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

        return True

    def get_and_save(self, endpoint: str, params: Dict) -> Dict:
        data = self.get_data(endpoint, params)
        self.save_data(data, endpoint)
        return data

    def process(self, endpoint: str, **params) -> bool:
        default = {"fields": "*", "limit": 500, "offset": 0, "order": "updated_at:desc"}
        default.update(params)

        while True:
            data = self.get_and_save(endpoint, default)

            try:
                updated_timestamp = int(data[-1]["updated_at"])
            except KeyError as err:
                print(err)
                print(data[-1].keys())
                updated_timestamp = int(datetime.now().timestamp()) - 100_000

            if (len(data) < 500) or (updated_timestamp < self.delay_timestamp):
                return True

            default["offset"] += default["limit"]
