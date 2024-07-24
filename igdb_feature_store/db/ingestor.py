import os
import json
from typing import Dict, List
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


class IGDBIngestor:

    def __init__(self, token: str, client_id: str, delay: int, path: str):

        self.token = token
        self.client_id = client_id
        self.delay = delay
        self.path = path

        self.base_url = "https://api.igdb.com/v4"
        self.headers = {"Client-ID": client_id, "Authorization": f"Bearer {token}"}
        self.delay_timestamp = int((datetime.now() - timedelta(days=delay)).timestamp())

    def get_data(self, endpoint: str, params: Dict = {}) -> List[Dict]:
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

    def get_and_save(self, endpoint: str, params: Dict) -> List[Dict]:
        try:
            data = self.get_data(endpoint, params)
            self.save_data(data, endpoint)

        except Exception as err:
            date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            error_msg = (
                "============= \n"
                f"[{date_time}] Request for endpoint {endpoint} with params {params} failed"
                f" with error \n{err}"
                "============= \n"
            )
            print(error_msg)
            data = [{}]

        return data

    def process(self, endpoint: str, **params) -> bool:
        default = {"fields": "*", "limit": 500, "offset": 0, "order": "updated_at:desc"}
        default.update(params)

        start_time = datetime.now()

        print(
            f"[{start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}] Start getting data for endpoint {endpoint}"
        )

        while True:
            data = self.get_and_save(endpoint, default)
            finish_time = datetime.now()
            elapsed_time = round((finish_time - start_time).total_seconds() / 60, 2)

            try:
                updated_timestamp = int(data[-1]["updated_at"])
                date_time = datetime.fromtimestamp(updated_timestamp).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
                print(
                    f"({date_time}) finished @ {finish_time.strftime('%Y-%m-%d %H:%M:%S')}"
                    f" ({elapsed_time} minutes since begining)"
                )
            except KeyError:
                updated_timestamp = int(datetime.now().timestamp()) - 100_000
                print(f'Endpoint {endpoint} does not have "updated_at" field.')

            if (len(data) < 500) or (updated_timestamp < self.delay_timestamp):
                print(
                    f"[{finish_time.strftime('%Y-%m-%d %H:%M:%S.%f')}] Finished etting data for endpoint {endpoint}"
                    f" ({elapsed_time} minutes since begining"
                )
                return True

            default["offset"] += default["limit"]
